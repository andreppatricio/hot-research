from airflow.decorators import dag, task

from datetime import datetime, timedelta

import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import core_api

from task_functions import filter_english_titles_task, construct_paper_table_task, \
                                    construct_journal_table_task, construct_author_table_task, \
                                    insert_in_db_task, save_file
from task_functions import get_connection_params_task, create_pool_func

from airflow.models import Pool
import psycopg2
import databases


pool_name = 'query_core_api_pool'
create_pool_func(pool_name, 'Pool to limit queries to the CORE API', slots=1)

API_NAME = 'core'
files_to_delete = []



##### DAG args ##############333

default_args = {
    'owner': 'alex',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

start_date_str = os.environ.get('START_DATE')
if start_date_str is not None:
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
else:
    start_date = datetime(2023,9,1)

@dag(
    description='Dag to get CORE API data',
    start_date= start_date, #datetime(2023, 9, 1),
    schedule="@weekly", # Sunday at 00:00
    catchup=True,
    default_args=default_args
)
def core_api_tf():

    @task()
    def define_date_interval(**kwargs):
        logical_date = kwargs['logical_date'].date()
        print("logical date", logical_date)
        # Calculate the start of the current week (Monday)
        week_start = logical_date - timedelta(days=logical_date.weekday())
        print("week start", week_start)

        # Calculate the end of this week (Sunday)
        week_end = week_start + timedelta(days=6)
        print("week end", week_end)

        return {'start_date': week_start.strftime("%Y-%m-%d"),
                'end_date': week_end.strftime("%Y-%m-%d")}


    @task(pool=pool_name)
    def get_api_data_task(dates):
        results = core_api.get_api_data(dates)
        response_json = {'results': results}
        results_file_name = save_file(response_json, 'core_api_response.json', add_id=True)
        return results_file_name


    @task()
    def create_schema_task():
        conn_params = {"host": "postgres", "database": "airflow", "user": "airflow", "password": "airflow"}
        databases.create_schema(conn_params)

    
    @task()
    def create_tables_task():
        conn_params = {"host": "postgres", "database": "airflow", "user": "airflow", "password": "airflow"}
        databases.create_tables(conn_params)


    @task()
    def trigger_arxiv_dag_task(dates, **kwargs):
        trigger_arxiv_dag_task = TriggerDagRunOperator(
                                    task_id="trigger_arxiv_dag_task",
                                    trigger_dag_id="arxiv_api_tf",     
                                    conf={"start_date": dates['start_date'], 
                                          "end_date": dates['end_date']}
                                )
        trigger_arxiv_dag_task.execute(context=kwargs) # type: ignore

    @task()
    def cleanup_files(files_to_delete: list):
        print("Cleaning UP these files: ", files_to_delete)
        for file in files_to_delete:
            if file is not None and os.path.exists(file):
                print("Removing: ", file)
                os.remove(file)


    # ----------------------------------- #

    with cleanup_files(files_to_delete).as_teardown():
        dates = define_date_interval()

        api_data_file = get_api_data_task(dates)

        filtered_api_data_file = filter_english_titles_task(api_data_file, API_NAME)
        paper_df_file = construct_paper_table_task(filtered_api_data_file, API_NAME)
        author_df_file = construct_author_table_task(filtered_api_data_file, API_NAME)
        journal_df_file = construct_journal_table_task(filtered_api_data_file, API_NAME)
        
        df_files = {'paper': paper_df_file, 'author': author_df_file, 'journal': journal_df_file}
        conn_params = get_connection_params_task()
        create_schema_task() >> create_tables_task() >> insert_in_db_task(df_files, conn_params) >> trigger_arxiv_dag_task(dates)

        # trigger_arxiv_dag(dates)

        files_to_delete.extend([api_data_file, filtered_api_data_file, 
                                paper_df_file, author_df_file, journal_df_file])
        
core_api_tf()