from datetime import datetime, timedelta
import psycopg2

from airflow.decorators import dag, task

import databases


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

conn_params = databases.get_connection_params()

@dag(
    description='Dag to reset Database',
    start_date= datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args=default_args
)
def reset_db_dag():

    @task()
    def reset_db_task():
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(RESET_DB_QUERY)
        conn.close()

    reset_db_task()

reset_db_dag()

