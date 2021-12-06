from jiffysql.gcp.logs import Logs
from jiffysql.gcp.fetch import Fetch
from jiffysql.tasks.tasks import get_tasks
from jiffysql.utils.format import enrich_request


class Jiffy:

    def __init__(self, config):
        self._config = enrich_request(config)
        self._repo_fetcher = Fetch(self._config)
        self._repo_fetcher.download_to_local()
        self._logs = Logs(config)

    def run(self, dry_run=False):
        if dry_run:
            self._run_process(dry_run=dry_run)
        else:
            self._logs.start()
            if self._logs.should_run_process():
                try:
                    self._run_process(dry_run=dry_run)
                    self._logs.stop(f'Process complete :)')
                except Exception as e:
                    self._logs.stop(f'{type(e).__name__}: {e}')
                    self._repo_fetcher.delete_local_repo()
                    raise e
            else:
                self._logs.stop('Tasks already running.')
        self._repo_fetcher.delete_local_repo()
        return True

    def _run_process(self, dry_run=False):
        all_tasks = get_tasks(request=self._config)
        for task_name in all_tasks['task_order']:
            all_tasks['tasks'][task_name].run(dry_run=dry_run)

