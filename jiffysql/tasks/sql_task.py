import re
from jiffysql.gcp.big_query import run_script
from jiffysql.tasks.base_sql_task import BaseSQLTask
from jiffysql.tasks.validation_task import ValidationTask

table_regex = re.compile(r'[FfJj][RrOo][OoIi][MmNn] `(.*\..*\..*)`')
file_name_regex = re.compile(r'[\\|\/]([0-9a-zA-Z_]*)\.sql')


class SQLTask(BaseSQLTask):

    def __init__(self, filename, params, request):
        super().__init__(filename, params, request)
        self.output_table_name = None
        self._validation_tasks = []
        self._get_output_table_name()
        self._get_validation_tasks()

    def run(self, dry_run=False):
        """
        Run SQL script to create a table if needed. If dry run, just run anyway.

        :return: None
        """
        if dry_run:
            run_script(query_data=self._query_data,
                       dry_run=dry_run)
        else:
            if self._table_has_not_been_run():
                run_script(self._query_data)
            self._run_tests_for_output_table()

    def _table_has_not_been_run(self):
        """
        If table passes all the validation tests then it does not need to be run.

        :return: True = table has not been run / False = table has been run
        """
        for validation_task in self._validation_tasks:
            if not validation_task.run_test():
                return True
        return False

    def _run_tests_for_output_table(self):
        """
        Run all validation tests and write the output to the validation table. Raise an error if any tests fail.

        :return: None
        """
        test_results = []
        for validation_task in self._validation_tasks:
            test_results.append(validation_task.run_test())
            validation_task.write_test_result_to_validation_table()
        if False in test_results:
            validation_table = f'{self._request["project"]}.{self._params["dataset"]}.validation_tests'
            raise RuntimeError(f'Validation tests fail! Please check {validation_table} for more information. '
                               f'Request that caused the failure: {self._request}')

    def get_dependencies(self):
        """
        Search through the SQL script for all BigQuery table references.

        Store those dependencies in a dict along with the output table name.

        :return: Dependencies of required tables and output table name
        """

        tables = table_regex.findall(self._query_data['sql'])
        dependencies = {
            'output_table': self.output_table_name,
            'source_tables': set(tables)
        }
        return dependencies

    def _get_output_table_name(self):
        """
        Build the fully qualified name of the BigQuery table that this task will create / write to.

        E.g.: cool-project.funky_dataset.300_solid_data

        :return: Full table name
        """

        table = file_name_regex.findall(self._filename)[0]
        dataset = self._params['dataset']
        project = self._request['project']
        self.output_table_name = f'{project}.{dataset}.{table}'
        self._query_data['job_settings']['table_name'] = self.output_table_name
        return self.output_table_name

    def _get_validation_tasks(self):
        """
        Gather all of the validation tasks required for this table.

        :return: List of validation tasks
        """

        js = self._query_data['job_settings'].get('VALIDATION', None)
        if js:
            for validation_name in js.split(','):
                validation_file_path = self._request['validation_files'][validation_name]
                validation_task = ValidationTask(filename=validation_file_path,
                                                 params=self._params,
                                                 request=self._request)
                self._validation_tasks.append(validation_task)
