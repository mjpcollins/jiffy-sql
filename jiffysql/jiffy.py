from jiffysql.gcp.fetch import Fetch
from jiffysql.tasks.tasks import get_tasks


class Jiffy:

    def __init__(self, config):
        self._config = config

    def run(self, dry_run=False):
        fetch_obj = Fetch(self._config)
        fetch_obj.delete_local_repo()
        fetch_obj.download_to_local()
        all_tasks = get_tasks(request=self._config)
        for task_name in all_tasks['task_order']:
            all_tasks['tasks'][task_name].run(dry_run=dry_run)
        fetch_obj.delete_local_repo()
        return True
