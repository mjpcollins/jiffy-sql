import re
import glob
from importlib import import_module
from jiffysql.tasks.sql_task import SQLTask
from jiffysql.utils.format import enrich_request
from jiffysql.graph.dependency_graph import DependencyGraph
from jiffysql.gcp.big_query import create_validation_output_table


def get_tasks(request):
    enrich_request(request)
    params = get_parameters(request)
    create_validation_output_table(request, params)
    request['validation_files'] = get_all_validation_files(request)
    sql_files = get_all_sql_files(request)
    task_data = get_task_data(params=params,
                              request=request,
                              sql_files=sql_files)
    return task_data


def get_task_data(params, request, sql_files):
    sql_tasks_lookup = {}
    dep_graph = DependencyGraph()
    for file in sql_files:
        sql_task = SQLTask(filename=file,
                           params=params,
                           request=request)
        dependencies = sql_task.get_dependencies()
        sql_tasks_lookup[sql_task.output_table_name] = sql_task
        dep_graph.add(dependencies)
    task_data = {'task_order': dep_graph.get_task_list(),
                 'tasks': sql_tasks_lookup}
    return task_data


def get_parameters(request):
    return import_module(request['params_import_path']).settings


def get_all_sql_files(request):
    folder = request["sql_file_path"]
    return get_all_sql_files_within_this_folder(folder)


def get_all_validation_files(request):
    folder = request['sql_validation_tests_file_path']
    sql_files_in_validation_folder = get_all_sql_files_within_this_folder(folder)
    return create_validation_file_dict(sql_files_in_validation_folder)


def create_validation_file_dict(sql_files_in_validation_folder):
    file_name_regex = re.compile(r'[\\|\/]([0-9a-zA-Z_]*)\.sql')
    validation_files = {}
    for filename in sql_files_in_validation_folder:
        validation_name = file_name_regex.findall(filename)[0]
        validation_files[validation_name] = filename
    return validation_files


def get_all_sql_files_within_this_folder(folder):
    all_files_in_sql_folder = glob.glob(f'{folder}**',
                                        recursive=True)
    all_sql_files = [f for f in all_files_in_sql_folder
                     if '.sql' == f[-4:]]
    return all_sql_files
