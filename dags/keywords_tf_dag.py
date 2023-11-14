from datetime import datetime, timedelta
import os

from airflow.decorators import dag, task

import databases
import processing
import task_functions as tf



files_to_delete = []


default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id = "keywords_dag_tf",
    description='Dag to process keywords',
    start_date=datetime(2020, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args
)
def keywords_dag_tf():

    @task()
    def get_date_interval(dag_run=None):
        start_date = dag_run.conf.get('start_date')
        end_date = dag_run.conf.get('end_date')
        return {'start_date': start_date, 'end_date': end_date}
    
    @task()
    def get_titles(dates, conn_params: dict):
        results = databases.get_titles_from_db(dates, conn_params)
        results_json = {'titles': results}
        results_file_name = tf.save_file(results_json, 'titles.json', add_id=True)
        return results_file_name
    
    @task()
    def extract_ngram_keywords(titles_file, n, n_top_keywords=100):
        titles = tf.load_json(titles_file)
        titles = titles['titles']
        keywords_dict = processing.extract_ngram_keywords(titles, n, n_top_keywords)

        results_file_name = tf.save_file(keywords_dict, f'top_{n}_grams.json', add_id=True)
        return results_file_name

    @task()
    def insert_keywords_db(keywords_file: str, dates, n, conn_params: dict):
        keywords_dict = tf.load_json(keywords_file)
        databases.insert_keywords_in_db(keywords_dict, dates, n, conn_params)


    @task()
    def cleanup_files(files_to_delete: list):
        for file in files_to_delete:
            if os.path.exists(file):
                os.remove(file)

    # ----------------------------------- #

    with cleanup_files(files_to_delete).as_teardown():
        dates = get_date_interval()
        conn_params = tf.get_connection_params_task()
        titles_file = get_titles(dates, conn_params)
        files_to_delete.append(titles_file)
        one_gram_file = extract_ngram_keywords(titles_file, n=1, n_top_keywords=200)
        two_gram_file = extract_ngram_keywords(titles_file, n=2, n_top_keywords=200)
        three_gram_file = extract_ngram_keywords(titles_file, n=3, n_top_keywords=200)
        files_to_delete.append(one_gram_file)
        files_to_delete.append(two_gram_file)
        files_to_delete.append(three_gram_file)
        insert_keywords_db(one_gram_file, dates, 1, conn_params)
        insert_keywords_db(two_gram_file, dates, 2, conn_params)
        insert_keywords_db(three_gram_file, dates, 3, conn_params)

keywords_dag_tf()
