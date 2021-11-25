import os
import shutil


def download_to_local(request):
    repo = request['repo']
    project = request['project']
    branch = request.get('branch', 'master')
    os.system(f'gcloud source repos clone {repo} --project={project}')
    if branch not in ('main', 'master'):
        print(f'Pulling from {branch} branch...')
        os.system(f'cd {repo}; git checkout {branch}')


def delete_local_repo(request):
    repo = request['repo']
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
