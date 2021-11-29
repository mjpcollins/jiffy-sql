import uuid
from jiffysql.gcp.logs import Logs
from unittest import TestCase, mock


TEST_UUIDS = [f'uuid_{i}' for i in range(10)]


class TestLogs(TestCase):

    @mock.patch.object(uuid, 'uuid4', side_effect=TEST_UUIDS)
    def setUp(self, _):
        mock.patch('jiffysql.gcp.logs.get_parameters', fake_get_parameters).start()
        mock.patch('jiffysql.gcp.logs.create_table', fake_create_table).start()
        mock.patch('jiffysql.gcp.logs.Client', fake_client).start()
        self._config = {
            'repo': 'a_repo',
            'params': 'some_params',
            'project': 'a_project',
            'params_import_path': 'a_repo.sql.some_params',
            'sql_file_path': 'a_repo/sql/',
            'sql_validation_tests_file_path': 'a_repo/sql_tests/'
        }
        self.logs = Logs(self._config)

    def test_init(self):
        self.assertEqual(self._config, self.logs._config)
        self.assertEqual({'dataset': 'a_dataset'}, self.logs._params)
        self.assertEqual('a_project.a_dataset.logs', self.logs._table)
        self.assertEqual('uuid_0', self.logs._run_id)


def fake_create_table(table_ref, schema):
    return None


def fake_get_parameters(config):
    return {'dataset': 'a_dataset'}


def fake_client(project):
    return None
