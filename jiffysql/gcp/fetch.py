import os
import shutil
import platform


class Fetch:

    def __init__(self, request):
        self._repo = request.get('repo')
        self._project = request.get('project')
        self._branch = request.get('branch', 'master')

    def download_to_local(self):
        self.delete_local_repo()
        clone_command = self._get_clone_command()
        os.system(clone_command)
        if self._branch not in ('main', 'master'):
            print(f'Pulling from {self._branch} branch...')
            branch_command = self._get_branch_checkout_command()
            os.system(branch_command)

    def delete_local_repo(self):
        try:
            shutil.rmtree(self._repo)
        except FileNotFoundError:
            pass

    def _get_branch_checkout_command(self):
        if platform.system() == 'Windows':
            command = f'cd {self._repo} && git checkout {self._branch}'
        else:
            command = f'cd {self._repo}; git checkout {self._branch}'
        return command

    def _get_clone_command(self):
        return f'gcloud source repos clone {self._repo} --project={self._project}'
