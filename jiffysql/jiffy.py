from jiffysql.gcp.fetch import (
    download_to_local,
    delete_local_repo
)
from jiffysql.tasks.tasks import get_tasks


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
