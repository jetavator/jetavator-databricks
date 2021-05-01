import base64
import os
import time
import pandas as pd
from copy import deepcopy

import jinja2
import nbformat
import sqlalchemy
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk import (
    ApiClient,
    JobsService,
    ManagedLibraryService,
    WorkspaceService,
    SecretService,
    ClusterService
)

from jetavator.config.secret_lookup import SecretLookup
from lazy_property import LazyProperty
from sqlalchemy.schema import CreateColumn

from jetavator import REQUIRED
from jetavator.config import Config, ServiceConfig
from jetavator.runners import Runner

mssql_dialect = sqlalchemy.databases.mssql.dialect()

JOB_POLL_FREQUENCY_SECS = 1
JOB_ERROR_MESSAGE_TIMEOUT = 10
CLUSTER_START_POLL_FREQUENCY_SECS = 5

DBFS_JOB_ROOT = 'dbfs:/jetavator/jobs'
DBFS_DATA_ROOT = 'dbfs:/jetavator/data'

NOTEBOOK_PATH_ROOT = '/jetavator/jobs'

LIFE_CYCLE_ERROR_STATES = [
    'INTERNAL_ERROR'
]

LIFE_CYCLE_INCOMPLETE_STATES = [
    'PENDING',
    'RUNNING'
]

RESULT_ERROR_STATES = [
    'FAILED',
    'TIMEDOUT',
    'CANCELED'
]

SQL_JAVA_TYPE_MAPPING = {
    'FLOAT': 'DOUBLE',
    'DATETIME': 'TIMESTAMP'
}

# TODO: We also need to build and upload a wheel for jetavator_databricks_local
# TODO: These dbutils calls should only be necessary in development, not prod
PYTHON_SETUP_SCRIPT = '''
%python

dbutils.library.install("{wheel_path}")
dbutils.library.restartPython()

import sys

from jetavator_databricks_local import *
from jetavator import Engine, Config

schema=dbutils.widgets.get('schema')
run_uuid=dbutils.widgets.get('run_uuid')

engine.config = Config._from_json_file(
    '/dbfs/jetavator/jobs/' + schema + '/config.json')

engine.config.schema = schema
engine.config.model_path = (
    '/dbfs/jetavator/jobs/' + schema + '/definitions/')
engine.config.session.run_uuid = run_uuid

jetavator_engine = Engine(engine.config)
'''

JETAVATOR_DEPLOY_SCRIPT = '''
jetavator_engine.deploy()
'''

JETAVATOR_RUN_SCRIPT = '''
jetavator_engine.run()
'''

# TODO: Make timeouts configurable

SCALA_SETUP_SCRIPT = '''
%scala

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import com.microsoft.azure.sqldb.spark.query._

val schema = dbutils.widgets.get("schema")
val runUuid = dbutils.widgets.get("run_uuid").replace("-", "_")

val sqlUsername = dbutils.secrets.get(scope="jetavator", key="MSSQL_USERNAME")
val sqlPassword = dbutils.secrets.get(scope="jetavator", key="MSSQL_PASSWORD")
val sqlDatabase = dbutils.secrets.get(scope="jetavator", key="MSSQL_DATABASE")
val sqlServer = dbutils.secrets.get(scope="jetavator", key="MSSQL_SERVER")

val configMap = Map(
  "url"            -> sqlServer,
  "databaseName"   -> sqlDatabase,
  "user"           -> sqlUsername,
  "password"       -> sqlPassword,
  "connectTimeout" -> "5",
  "queryTimeout"   -> "300"
)

def retry[T](n: Int)(fn: => T): T = {
  try {
    fn
  } catch {
    case e : Throwable =>
      if (n > 1) retry(n - 1)(fn)
      else throw e
  }
}
'''

SQL_TEMPLATE = '''

val query = """
{sql}
"""

val queryConfig = Config(configMap + ("queryCustom" -> query))

retry(20) {{
    sqlContext.sqlDBQuery(queryConfig)
}}
'''

BULK_COPY_TEMPLATE = '''

retry(20) {
    sqlContext.sqlDBQuery(Config(configMap + ("queryCustom" -> ("""
    DROP TABLE IF EXISTS
    """ + schema + ".updates_{{table_name}}_" + runUuid))))
}

retry(20) {
    sqlContext.sqlDBQuery(Config(configMap + ("queryCustom" -> ("""
    CREATE TABLE
    """ + schema + ".updates_{{table_name}}_" + runUuid +
    """
    (
        {% for column in columns %}
        {{ column.definition }}{{ "," if not loop.last}}
        {%- endfor %}
    )
    """))))
}

val df = spark.sql("""
SELECT {% for column in columns %}
       {{ column.name }}{{ "," if not loop.last}}
       {%- endfor %}
  FROM updates_{{table_name}}
""")

var bulkCopyMetadata = new BulkCopyMetadata
{% for column in columns %}
bulkCopyMetadata.addColumnMetadata(
    {{loop.index}},
    "{{column.name}}",
    java.sql.Types.{{column.type}},
    {{column.precision}},
    {{column.scale}}
)
{% endfor %}

val bulkCopyConfig = Config(configMap + (
  "dbTable"           -> (schema + ".updates_{{table_name}}_" + runUuid),
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

retry(20) {
    df.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)
}

retry(20) {
    sqlContext.sqlDBQuery(Config(configMap + ("queryCustom" -> ("""
    MERGE
     INTO
    """ + schema + ".{{table_name}}" +
    """
    AS target
    USING
    """ + schema + ".updates_{{table_name}}_" + runUuid +
    """
    AS source
       ON source.{{satellite_owner.key_column_name}}
        = target.{{satellite_owner.key_column_name}}
          WHEN MATCHED AND source.deleted_ind = 1 THEN DELETE
          {% if star_columns | length > 0 %}
          WHEN MATCHED THEN UPDATE SET
               {% for column in star_columns %}
               target.{{column.name}} = (
                   CASE WHEN source.{{column.update_ind}} = 1
                        THEN source.{{column.name}}
                        ELSE target.{{column.name}}
                        END){{"," if not loop.last}}
               {% endfor %}
          {% endif %}
          WHEN NOT MATCHED THEN INSERT (
              {% for column in satellite_owner.sql_model.star_table.c %}
              {{column.name}}{{"," if not loop.last}}
              {%- endfor %}
          )
          VALUES
          (
              {% for column in satellite_owner.sql_model.star_table.c %}
              source.{{column.name}}{{"," if not loop.last}}
              {%- endfor %}
          );
    """))))
}

retry(20) {
    sqlContext.sqlDBQuery(Config(configMap + ("queryCustom" -> ("""
    DROP TABLE
    """ + schema + ".updates_{{table_name}}_" + runUuid))))
}
'''


class Timeout(object):

    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        self.expiry_time = time.time() + self.seconds
        return self

    def __exit__(self, type_, value, traceback):
        pass

    @property
    def timed_out(self):
        return time.time() > self.expiry_time


class DatabricksJob(object):

    def __init__(self, runner, filename, scripts):
        self.runner = runner
        self.filename = filename
        self.scripts = scripts

    @LazyProperty
    def jupyter_notebook(self):
        return nbformat.v4.new_notebook(
            cells=[
                nbformat.v4.new_code_cell(script)
                for script in self.scripts
            ]
        )

    @property
    def notebook_dir(self):
        return '/'.join([
            NOTEBOOK_PATH_ROOT,
            self.runner.config.vault_schema
        ])

    @property
    def notebook_path(self):
        return '/'.join([
            self.notebook_dir,
            self.filename
        ])

    def create(self):
        self.logger.info(f'Deploying script: {self.filename}')
        self.runner.workspace_api.mkdirs(self.notebook_dir)
        self.runner.workspace_api.import_workspace(
            path=self.notebook_path,
            format='JUPYTER',
            content=self.runner.encode_contents(
                nbformat.writes(self.jupyter_notebook)),
            overwrite=True
        )

    def run(self):
        self.logger.info(
            f'Running {self.filename} on remote databricks '
            f'with run_uuid [{self.runner.engine.config.session.run_uuid}]'
        )
        job_run = self.runner.jobs_api.submit_run(
            run_name=self.filename,
            existing_cluster_id=self.runner.config.cluster_id,
            notebook_task={
                'notebook_path': self.notebook_path,
                'base_parameters': {
                    'schema': self.runner.config.vault_schema,
                    'run_uuid': self.runner.engine.config.session.run_uuid
                }
            },
            libraries=[
                {'whl': self.runner.dbfs_wheel_path},
                *[{'pypi': {'package': x}} for x in REQUIRED],
                *self.runner.config.libraries
            ]
        )
        run_id = job_run['run_id']
        life_cycle_state = 'PENDING'
        while life_cycle_state in LIFE_CYCLE_INCOMPLETE_STATES:
            time.sleep(JOB_POLL_FREQUENCY_SECS)
            run_output = self.runner.jobs_api.get_run_output(run_id)
            run_state = run_output['metadata']['state']
            life_cycle_state = run_state['life_cycle_state']
            if (
                life_cycle_state in LIFE_CYCLE_ERROR_STATES
                or run_state.get('result_state') in RESULT_ERROR_STATES
            ):
                with Timeout(JOB_ERROR_MESSAGE_TIMEOUT) as t:
                    messages = []
                    while not (t.timed_out or any(
                        'ERROR' in message for message in messages
                    )):
                        messages = list(self.runner.compute_service.log_listener)
                        for message in messages:
                            self.logger.info(message)
                raise Exception(f'''
                Job completed with error:
                {run_output}
                ''')
            self.print_log_messages()
        time.sleep(JOB_POLL_FREQUENCY_SECS)
        self.print_log_messages()
        self.runner.compute_service.log_listener.delete_queue()
        self.logger.info('\nRun complete')

    def print_log_messages(self):
        for message in self.runner.compute_service.log_listener:
            self.logger.info(message)


class DatabricksRunner(Runner, register_as='remote_databricks'):

    def performance_data(self) -> pd.DataFrame:
        raise NotImplementedError

    @property
    def config(self):
        return self.compute_service.config

    @LazyProperty
    def dbfs_api(self):
        return DbfsApi(self.databricks_api)

    @LazyProperty
    def jobs_api(self):
        return JobsService(self.databricks_api)

    @LazyProperty
    def libraries_api(self):
        return ManagedLibraryService(self.databricks_api)

    @LazyProperty
    def workspace_api(self):
        return WorkspaceService(self.databricks_api)

    @LazyProperty
    def secrets_api(self):
        return SecretService(self.databricks_api)

    @LazyProperty
    def clusters_api(self):
        return ClusterService(self.databricks_api)

    @LazyProperty
    def databricks_api(self):
        return ApiClient(
            host=f'https://{self.config.host}',
            token=self.config.token
        )

    def load_csv(self, csv_file, source):
        self.logger.info(f"{source.name}.csv: Uploading file")
        with open(csv_file, "rb") as data:
            self.engine.source_storage_service.upload_blob(
                filename=(
                    f'{self.config.vault_schema}/'
                    f'{self.engine.config.session.run_uuid}/'
                    f'{source.name}.csv'
                ),
                data=data
            )

    @property
    def dbfs_definitions_dir(self):
        return (
            f'{DBFS_JOB_ROOT}/{self.config.vault_schema}/definitions/'
        )

    def clear_yaml(self):
        self.dbfs_api.delete(
            DbfsPath(self.dbfs_definitions_dir),
            recursive=True
        )

    def load_yaml(self, relative_path):
        dbfs_path = self.dbfs_definitions_dir + relative_path.replace(
            '\\', '/')
        self.logger.info(f'Uploading YAML: {dbfs_path}')
        self.dbfs_api.put_file(
            os.path.join(self.engine.config.model_path, relative_path),
            DbfsPath(dbfs_path),
            overwrite=True
        )

    @property
    def dbfs_wheel_path(self):
        return (
            'dbfs:/jetavator/lib/' +
            os.path.basename(self.engine.config.wheel_path)
        )

    def build_remote_config(self):
        # noinspection PyProtectedMember
        # TODO: Refactor so this doesn't access protected member Config._properties
        remote_config = Config({
            k: v
            for k, v in deepcopy(self.engine.config).items()
            if k in Config._properties
        })
        databricks_services = [
            service
            for service in remote_config.services.values()
            if service.type == 'remote_databricks'
        ]
        for service in databricks_services:
            remote_config.services[service.name] = ServiceConfig({
                'name': service.name,
                'type': 'local_databricks'
            })
        remote_config.secret_lookup = 'databricks'
        return remote_config

    def load_config(self):
        dbfs_path = f'{DBFS_JOB_ROOT}/{self.config.vault_schema}/config.json'
        self.logger.info(f'Uploading config: {dbfs_path}')
        # noinspection PyProtectedMember
        # TODO: Refactor so this doesn't access protected member _to_json
        self.save_as_dbfs_file(
            dbfs_path,
            self.build_remote_config()._to_json()
        )

    def create_secrets(self):
        self.logger.info(f'Creating secrets')
        scope_name = 'jetavator'
        if self.secrets_api.list_scopes():
            if any(
                scope['name'] == scope_name
                for scope in
                self.secrets_api.list_scopes()['scopes']
            ):
                self.secrets_api.delete_scope(scope_name)
        self.secrets_api.create_scope(
            scope_name,
            initial_manage_principal='users'
        )
        secret_lookup = SecretLookup.registered_subclass_instance(
            self.engine.config.secret_lookup
        )
        for element, _, _, _ in self.engine.config.walk_elements():
            if isinstance(element, str):
                secret = secret_lookup.get_secret_name(element)
                if secret:
                    self.secrets_api.put_secret(
                        scope_name,
                        secret,
                        secret_lookup(element)
                    )

    def load_wheel(self):
        self.logger.info(
            f'Writing wheel to {self.dbfs_wheel_path}')
        self.dbfs_api.put_file(
            self.engine.config.wheel_path,
            DbfsPath(self.dbfs_wheel_path),
            overwrite=True
        )
        cluster_status = self.libraries_api.cluster_status(
            self.config.cluster_id)
        to_uninstall = [
            library['library']
            for library in cluster_status['library_statuses']
            if library['library'].get('whl')
            and 'jetavator' in library['library'].get('whl', '')
            and library['library'].get('whl') != self.dbfs_wheel_path
        ]
        if to_uninstall:
            self.logger.info(
                'Uninstalling previous versions of library: '
                f'{[x["whl"] for x in to_uninstall]}'
            )
            self.libraries_api.uninstall_libraries(
                self.config.cluster_id,
                to_uninstall
            )
        self.logger.info(f'Requesting wheel installation')
        self.libraries_api.install_libraries(
            self.config.cluster_id,
            libraries={'whl': self.dbfs_wheel_path}
        )

    def create_jobs(self):
        self.deploy_job.create()
        self.run_job.create()

    @property
    def python_setup_script(self):
        return PYTHON_SETUP_SCRIPT.format(
            wheel_path=self.dbfs_wheel_path
        )

    @staticmethod
    def mssql_create_tables(table_create_statements):
        return [
            SCALA_SETUP_SCRIPT + SQL_TEMPLATE.format(
                sql=str(statement.compile(dialect=mssql_dialect))
            )
            for statement in table_create_statements
        ]

    def mssql_bulk_load_tables(self, satellite_owners):
        return [
            SCALA_SETUP_SCRIPT + jinja2.Template(BULK_COPY_TEMPLATE).render(
                schema=self.engine.config.vault_schema,
                table_name=satellite_owner.sql_model.star_table_name,
                columns=[
                    {
                        'name': column.name,
                        'type': SQL_JAVA_TYPE_MAPPING.get(
                            column.type.__class__.__name__,
                            column.type.__class__.__name__
                        ),
                        'precision': getattr(
                            column.type,
                            'precision',
                            getattr(column.type, 'length', 0)
                        ) or 0,
                        'scale': getattr(column.type, 'scale', 0) or 0,
                        'definition': str(CreateColumn(column).compile(
                            dialect=mssql_dialect))
                    }
                    for column in satellite_owner.sql_model.star_updates_table.columns
                ],
                star_columns=[
                    {
                        'name': column_name,
                        'update_ind': f'update_ind_{satellite.name}'
                    }
                    for satellite in satellite_owner.star_satellites.values()
                    for column_name in satellite.columns.keys()
                ],
                satellite_owner=satellite_owner
            )
            for satellite_owner in satellite_owners.values()
        ]

    def mssql_create_schema(self):
        return SCALA_SETUP_SCRIPT + SQL_TEMPLATE.format(
            sql=f'CREATE SCHEMA {self.engine.config.vault_schema}'
        )

    @LazyProperty
    def deploy_job(self):
        return DatabricksJob(
            self,
            'deploy.ipynb',
            scripts=[
                self.python_setup_script + JETAVATOR_DEPLOY_SCRIPT,
                self.mssql_create_schema(),
                *self.mssql_create_tables(
                    self.engine.sql_model.create_star_tables_ddl(
                        action='create',
                        with_index=True
                    )
                )
            ]
        )

    @LazyProperty
    def run_job(self):
        return DatabricksJob(
            self,
            'run.ipynb',
            scripts=[
                self.python_setup_script + JETAVATOR_RUN_SCRIPT,
                *self.mssql_bulk_load_tables({
                    k: v
                    for k, v in (
                        self
                        .engine
                        .schema_registry
                        .changeset
                        .satellite_owners
                        .items()
                    )
                    if v.satellites_containing_keys
                })
            ]
        )

    def deploy_remote(self):
        self.deploy_job.run()

    def run(self) -> None:
        self.run_job.run()

    @staticmethod
    def encode_contents(file_text):
        return base64.b64encode(file_text.encode('utf-8')).decode()

    def save_as_dbfs_file(self, dbfs_filename, file_text):
        self.dbfs_api.client.put(
            dbfs_filename,
            self.encode_contents(file_text),
            overwrite=True
        )

    def get_cluster_state(self):
        return self.clusters_api.get_cluster(self.config.cluster_id)['state']

    def start_cluster(self):
        state = self.get_cluster_state()
        while state == 'TERMINATING':
            self.logger.info('Waiting for cluster to terminate')
            time.sleep(CLUSTER_START_POLL_FREQUENCY_SECS)
            state = self.get_cluster_state()
        if state == 'TERMINATED':
            self.logger.info('Starting cluster')
            self.clusters_api.start_cluster(self.config.cluster_id)
            state = self.get_cluster_state()
        while state in ('PENDING', 'RESTARTING'):
            self.logger.info('Waiting for cluster to start')
            time.sleep(CLUSTER_START_POLL_FREQUENCY_SECS)
            state = self.get_cluster_state()
        if state in ('RUNNING', 'RESIZING'):
            self.logger.info('Cluster is running')
        else:
            raise Exception(f'Cluster in unexpected state: {state}')
