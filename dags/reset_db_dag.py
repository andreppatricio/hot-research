from datetime import datetime, timedelta
import psycopg2

from airflow.decorators import dag, task

import task_functions as tf


##### SQL queries ##################

RESET_DB_QUERY = """
    DROP TABLE IF EXISTS papers.author;
    DROP TABLE IF EXISTS papers.journal;
    DROP TABLE IF EXISTS papers.paper;
    DROP TABLE IF EXISTS papers.keyword;
    DROP SCHEMA IF EXISTS papers;

"""

default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    description='Dag to reset Database',
    start_date= datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args=default_args
)
def reset_db_dag():

    @task()
    def reset_db_task(conn_params):
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(RESET_DB_QUERY)
        conn.close()

    conn_params = tf.get_connection_params_task()
    reset_db_task(conn_params)

reset_db_dag()

