from jiffysql.gcp.fetch import (
    download_to_local,
    delete_local_repo
)
from jiffysql.tasks.tasks import get_tasks


class Jiffy:

    def __init__(self, config):
        self._config = config

    def run(self, dry_run=False):
        delete_local_repo(self._config)
        download_to_local(self._config)
        all_tasks = get_tasks(request=self._config)
        for task_name in all_tasks['task_order']:
            all_tasks['tasks'][task_name].run(dry_run=dry_run)
        delete_local_repo(self._config)
        return True
