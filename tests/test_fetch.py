from unittest import (
    TestCase,
    mock
)
from jiffysql.gcp.fetch import Fetch


class TestFetch(TestCase):

    def setUp(self):
        self.request = {
            'repo': 'a_repo',
            'branch': 'feature/a_branch',
            'project': 'a-project'
        }
        self.f = Fetch(self.request)
        self.repo = self.request['repo']
        self.branch = self.request['branch']

    def test_init(self):
        self.assertEqual('a_repo', self.f._repo)
        self.assertEqual('a-project', self.f._project)
        self.assertEqual('feature/a_branch', self.f._branch)

    @mock.patch("platform.system", mock.MagicMock(return_value='Windows'))
    def test_get_branch_checkout_command_windows(self):
        expected_result = 'cd a_repo && git checkout feature/a_branch'
        actual_result = self.f._get_branch_checkout_command()
        self.assertEqual(expected_result, actual_result)

    @mock.patch("platform.system", mock.MagicMock(return_value='Darwin'))
    def test_get_branch_checkout_command_mac(self):
        expected_result = 'cd a_repo; git checkout feature/a_branch'
        actual_result = self.f._get_branch_checkout_command()
        self.assertEqual(expected_result, actual_result)

    def test_get_clone_command(self):
        expected_result = 'gcloud source repos clone a_repo --project=a-project'
        actual_result = self.f._get_clone_command()
        self.assertEqual(expected_result, actual_result)
