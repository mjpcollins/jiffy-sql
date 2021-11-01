from unittest import TestCase
from jiffysql.tasks.tasks import (
    get_parameters,
    create_validation_file_dict
)


class TestTasks(TestCase):

    def setUp(self):
        self.request = {'repo': 'example-repo',
                        'params': 'params',
                        'params_import_path': 'example-repo.sql.params',
                        'sql_file_path': 'example-repo/sql/'}

    def test_get_parameters_no_input(self):
        expected_dynamic_import = {'data': 'that_is_imported_dynamically',
                                   'maths': 100}
        actual_dynamic_import = get_parameters(self.request)
        self.assertEqual(expected_dynamic_import, actual_dynamic_import)

    def test_get_all_validation_files(self):
        input_files = [
            'jiffy-sql-pilot/sql_tests\\100_raw_data_validation.sql',
            'jiffy-sql-pilot/sql_tests/200_raw_data_validation.sql',
        ]
        expected_dict = {'100_raw_data_validation': 'jiffy-sql-pilot/sql_tests\\100_raw_data_validation.sql',
                         '200_raw_data_validation': 'jiffy-sql-pilot/sql_tests/200_raw_data_validation.sql'}
        actual_dict = create_validation_file_dict(input_files)
        self.assertEqual(expected_dict, actual_dict)
