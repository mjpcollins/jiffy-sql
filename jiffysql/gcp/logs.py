import uuid
import datetime
from jiffysql.tasks.tasks import get_parameters
from jiffysql.gcp.big_query import (
    create_table,
    insert_into_table
)
from google.cloud.bigquery import Client
from jiffysql.schema.logs_table import schema as logs_schema


class Logs:

    def __init__(self, config):
        self._config = config
        self._params = get_parameters(self._config)
        self._table = self._get_output_table()
        self._client = Client(self._config["project"])
        self._run_id = self._get_run_id()
        create_table(
            project=self._config.get('project'),
            table_ref=self._table,
            schema=logs_schema
        )

    def start(self):
        self._write(
            category='start',
            message=str(self._config)
        )

    def log(self, message):
        self._write(
            category='log',
            message=message
        )

    def stop(self, reason):
        self._write(
            category='stop',
            message=reason
        )

    def should_run_process(self):
        query = f"""
        WITH started_processes AS (
            SELECT 
                run_id,
                write_time
            FROM {self._table}
            WHERE log_category = 'start'
        )
        
        , finished_processes AS (
            SELECT 
                run_id,
                write_time 
            FROM {self._table}
            WHERE log_category = 'stop'
        )
        
        SELECT 
            s.run_id,
            s.write_time
        FROM started_processes AS s 
        LEFT JOIN finished_processes AS f
        ON s.run_id = f.run_id
        WHERE f.run_id IS NULL
        ORDER BY s.write_time     
        """
        for row in self._client.query(query).result():
            return row.get('run_id') == self._run_id

    def _write(self, category, message):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row = [
            {
                'repository': self._config['repo'],
                'log_category': category,
                'message': message,
                'run_id': self._run_id,
                'write_time': now
            }
        ]
        insert_into_table(
            project=self._config["project"],
            table_ref=self._table,
            row=row
        )

    def _get_output_table(self):
        return f'{self._config["project"]}.{self._params["dataset"]}.logs'

    @staticmethod
    def _get_run_id():
        return str(uuid.uuid4())
