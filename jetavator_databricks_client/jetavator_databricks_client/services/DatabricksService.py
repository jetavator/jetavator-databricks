import sqlalchemy
import os
import time
import thrift

from lazy_property import LazyProperty

from jetavator.services.SparkService import SparkService

from TCLIService.ttypes import TOperationState

from functools import wraps

from .. import LogListener


CLUSTER_RETRY_INTERVAL_SECS = 15

# TODO: Move all the constants and file path generation into the same place
DBFS_DATA_ROOT = 'dbfs:/jetavator/data'


def retry_if_cluster_not_ready(original_function):
    @wraps(original_function)
    def decorated_function(self, *args, **kwargs):
        retry = True
        while retry:
            retry = False
            try:
                return original_function(self, *args, **kwargs)
            except thrift.Thrift.TApplicationException as e:
                if "TEMPORARILY_UNAVAILABLE" in str(e):
                    self.logger.info(
                        "Cluster is starting: retrying in 15 seconds")
                    time.sleep(CLUSTER_RETRY_INTERVAL_SECS)
                    retry = True
    return decorated_function


class DatabricksService(SparkService, register_as='remote_databricks'):

    def __init__(self, owner, config):
        super().__init__(owner, config)

    # TODO: Reverse this relationship so the Runner instantiates the Service
    @LazyProperty
    def databricks_runner(self):
        raise NotImplementedError

    @LazyProperty
    def log_listener(self):
        return LogListener(self.owner.config, self.owner.logs_storage_service)

    def yaml_file_paths(self):
        for root, _, files in os.walk(self.owner.config.model_path):
            for yaml_file in filter(
                lambda x: os.path.splitext(x)[1] == '.yaml',
                files
            ):
                yield os.path.join(root, yaml_file)

    def deploy(self):
        self.databricks_runner.create_jobs()
        self.databricks_runner.load_config()
        self.databricks_runner.create_secrets()
        self.databricks_runner.clear_yaml()
        for path in self.yaml_file_paths():
            self.databricks_runner.load_yaml(
                os.path.relpath(path, self.owner.config.model_path))
        self.databricks_runner.deploy_remote()

    def csv_file_path(self, source):
        return (
            f'{DBFS_DATA_ROOT}/'
            f'{self.config.schema}/'
            f'{source.name}.csv'
        )

    # TODO: Either implement this - or refactor so this class
    #       doesn't inherit this from SparkService
    def source_csv_exists(self, source):
        raise NotImplementedError

    def load_csv(self, csv_file, source):
        self.databricks_runner.load_csv(csv_file, source)

    def run_remote(self):
        self.databricks_runner.start_cluster()
        self.databricks_runner.run_remote()

    @property
    def spark(self):
        # TODO: Doesn't makes sense in this context - remove this property from the superclass
        #       or otherwise change the inheritance structure to avoid this
        raise NotImplementedError()
