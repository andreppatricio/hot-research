from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import arxiv_api
import pandas as pd
from airflow.models import Variable
import os

from task_functions import filter_english_titles_task, construct_paper_table_task, \
                                    construct_journal_table_task, construct_author_table_task, \
                                    insert_in_db_task, save_file
from task_functions import get_connection_params_task, create_pool_func

from airflow.models import Pool


# Define a pool
pool_name = 'query_arxiv_api_pool'
create_pool_func(pool_name, 'Pool to limit queries to the arxiv API', slots=1)

    
API_NAME = 'arxiv'
files_to_delete = []


default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id = 'arxiv_api_tf',
    description='Dag to get arxiv API data',
    start_date=datetime(2020, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args
)
def arxiv_api_tf():

    @task()
    def get_date_interval(dag_run=None):
        start_date = dag_run.conf.get('start_date')
        end_date = dag_run.conf.get('end_date')
        return {'start_date': start_date, 'end_date': end_date}
    
    @task(pool=pool_name)
    def get_api_data_task(dates):
        results = arxiv_api.get_api_data(dates)
        response_json = {'results': results}
        results_file_name = save_file(response_json, 'arxiv_api_response.json', add_id=True)
        return results_file_name


    @task()
    def trigger_keywords_dag(dates, **kwargs):
        trigger_keywords_dag = TriggerDagRunOperator(
                                    task_id="trigger_keywords_dag",
                                    # execution_date=datetime.now(),
                                    trigger_dag_id="keywords_dag_tf",  # dag to trigger   
                                    conf={"start_date": dates['start_date'], 
                                          "end_date": dates['end_date']}   
                                )
        trigger_keywords_dag.execute(context=kwargs) # type: ignore

    @task()
    def cleanup_files(files_to_delete: list):
        print("Cleaning UP these files: ", files_to_delete)
        for file in files_to_delete:
            if file is not None and os.path.exists(file):
                print("Removing: ", file)
                os.remove(file)

    # ----------------------------------- #

    with cleanup_files(files_to_delete).as_teardown():
        dates = get_date_interval()
        api_data_file = get_api_data_task(dates)
        filtered_api_data_file = filter_english_titles_task(api_data_file, API_NAME)
        paper_df_file = construct_paper_table_task(filtered_api_data_file, API_NAME)
        author_df_file = construct_author_table_task(filtered_api_data_file, API_NAME)
        journal_df_file = construct_journal_table_task(filtered_api_data_file, API_NAME)
        
        df_files = {'paper': paper_df_file, 'author': author_df_file, 'journal': journal_df_file}
        conn_params = get_connection_params_task()
        insert_in_db_task(df_files, conn_params) >> trigger_keywords_dag(dates)

        files_to_delete.extend([api_data_file, filtered_api_data_file, 
                                paper_df_file, author_df_file, journal_df_file]) 
        # trigger_keywords_dag(dates)

arxiv_api_tf()