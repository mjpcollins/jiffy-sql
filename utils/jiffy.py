from utils.fetch import (
    download_to_local,
    delete_local_repo
)
from utils.tasks import get_tasks


class Jiffy:

    def __init__(self, config):
        self._config = config

    def run(self):
        delete_local_repo(self._config)
        download_to_local(self._config)
        all_tasks = get_tasks(self._config)
        for task_name in all_tasks['task_order']:
            all_tasks['tasks'][task_name].run()
        delete_local_repo(self._config)
        return True


if __name__ == '__main__':
    conf = {'repo': 'example-sql-for-sql-runner',
             'project': 'national-rail-247416'}
    j = Jiffy(conf)
    j.run()
