import os

from jetavator.services import SparkService
from lazy_property import LazyProperty
from pyspark.sql import SparkSession

SPARK_APP_NAME = 'jetavator'


class LocalDatabricksService(SparkService, register_as="local_databricks"):

    @property
    def logger_config(self):
        return {
            'version': 1,
            'formatters': {
                'verbose': {
                    'format': '%(asctime)s %(levelname)s %(hostname)s %(process)d %(message)s',
                },
            },
            'handlers': {
                'queue': {
                    'service': self.owner.logs_storage_service,
                    'protocol': 'https',
                    'queue': f'jetavator-log-{self.owner.config.session.run_uuid}',
                    'level': 'DEBUG',
                    'class': 'jetavator_databricks_local.logging.azure_queue_logging.AzureQueueHandler',
                    'formatter': 'verbose',
                },
            },
            'loggers': {
                'jetavator': {
                    'handlers': ['queue'],
                    'level': 'DEBUG',
                },
            }
        }

    @property
    def azure_storage_key(self):
        return self.owner.source_storage_service.config.account_key

    @property
    def azure_storage_container(self):
        return self.owner.source_storage_service.config.blob_container_name

    @property
    def azure_storage_location(self):
        return (
            f'{self.owner.source_storage_service.config.account_name}.'
            'blob.core.windows.net'
        )

    @LazyProperty
    def dbutils(self):
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

    @property
    def spark(self):
        spark_session = (
            SparkSession
            .builder
            .appName(SPARK_APP_NAME)
            .enableHiveSupport()
            .getOrCreate()
        )
        storage_key_name = f'fs.azure.account.key.{self.azure_storage_location}'
        mount_point = f'/mnt/{self.azure_storage_container}'
        if not os.path.exists(f'/dbfs/{mount_point}'):
            self.owner.source_storage_service.create_container_if_not_exists()
            self.dbutils.fs.mount(
                source=(
                    f'wasbs://{self.azure_storage_container}@'
                    f'{self.azure_storage_location}'
                ),
                mount_point=mount_point,
                extra_configs={
                    storage_key_name: self.azure_storage_key
                }
            )
        return spark_session

    def csv_file_path(self, source_name: str):
        return (
            f'/mnt/{self.azure_storage_container}/'
            f'{self.config.schema}/'
            f'{self.owner.config.session.run_uuid}/'
            f'{source_name}.csv'
        )

    def source_csv_exists(self, source_name: str):
        return os.path.exists('/dbfs/' + self.csv_file_path(source_name))

    def load_csv(self, csv_file, source_name: str):
        # TODO: Either implement this or remove if from the superclass interface
        raise NotImplementedError()
