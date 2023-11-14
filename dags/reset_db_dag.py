from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import dag, task
import psycopg2
import databases


##### SQL queries ##################

RESET_DB_QUERY = """
    DROP TABLE IF EXISTS papers.author;
    DROP TABLE IF EXISTS papers.journal;
    DROP TABLE IF EXISTS papers.paper;
    DROP TABLE IF EXISTS papers.keyword;
    DROP SCHEMA IF EXISTS papers;

"""

# postgres_connection_id = 'postgres_paper_db_connection'

##### DAG args ##############333

default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

conn_params = databases.get_connection_params()

@dag(
    description='Dag to reset Database',
    start_date= datetime(2020, 2, 2),
    schedule="@once", # Sunday at 00:00
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

