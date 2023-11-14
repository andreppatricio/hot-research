import json
import pandas as pd
import uuid
from typing import Dict, Union
import os

import processing
import databases

from airflow.decorators import task
from airflow.api.common.experimental.pool import get_pool, create_pool
from airflow.exceptions import PoolNotFound


def load_json(filename: str) -> dict:
    """
    Load JSON data from a file.
    """
    with open(filename, 'r') as f:
        data = json.load(f)
    return data

def save_file(data: Union[dict, pd.DataFrame] ,filename: str, add_id = True) -> str:
    """
    Save data to a file, either as JSON or CSV.

    Raises:
        ValueError: If the data type is not supported.

    Example:
        >> data_dict = {'key': 'value'}
        >> save_file(data_dict, 'output.json')

        >> data_df = pd.DataFrame({'column': [1, 2, 3]})
        >> save_file(data_df, 'output.csv')
    """
    if add_id:
        filename = f"{str(uuid.uuid4().hex)}_{filename}"

    if isinstance(data, dict):
        with open(filename, 'w') as f:
            json.dump(data, f)
    elif isinstance(data, pd.DataFrame):
        data.to_csv(filename, index=False)
    else:
        raise ValueError(f"Argument 'data' should be either dict or pd.DataFrame, not {type(data)}")
    return filename


@task()
def filter_english_titles_task(api_data_file: str, api_name: str) -> str:
    """
    Filter English titles from API data and save the results to a file.

    Args:
        api_data_file (str): The path to the API data JSON file.
        api_name (str): The name to be used in the output filename.

    Returns:
        str: The filename of the saved file with filtered data.

    Example:
        >> result_file = filter_english_titles_task('api_data.json', 'example_api')
    """
    api_data = load_json(api_data_file)
    filtered_api_data = processing.filter_english_titles(api_data)
    results_file_name = save_file(filtered_api_data, f'{api_name}_filtered_api_response.json', add_id=True)
    return results_file_name


@task()
def construct_paper_table_task(api_data_file: str, api_name: str) -> str:
    """
    Construct a paper table from API data and save the results to a CSV file.

    Args:
        api_data_file (str): The path to the API data JSON file.
        api_name (str): The name to be used in the output filename.

    Returns:
        str: The filename of the saved CSV file with the constructed paper table.

    Example:
        >> result_file = construct_paper_table_task('api_data.json', 'example_api')
    """
    api_data = load_json(api_data_file)
    df = processing.construct_paper_table(api_data['results'])
    results_file_name = save_file(df, f"{api_name}_paper_df.csv", add_id=True)
    return results_file_name

@task()
def construct_author_table_task(api_data_file: str, api_name: str) -> str:
    """
    Construct an author table from API data and save the results to a CSV file.

    Args:
        api_data_file (str): The path to the API data JSON file.
        api_name (str): The name to be used in the output filename.

    Returns:
        str: The filename of the saved CSV file with the constructed author table.

    Example:
        >> result_file = construct_author_table_task('api_data.json', 'example_api')
    """
    api_data = load_json(api_data_file)
    df = processing.construct_author_table(api_data['results'])
    results_file_name = save_file(df, f"{api_name}_author_df.csv", add_id=True)
    return results_file_name

@task()
def construct_journal_table_task(api_data_file: str, api_name: str) -> str:
    """
    Construct a journal table from API data and save the results to a CSV file.

    Args:
        api_data_file (str): The path to the API data JSON file.
        api_name (str): The name to be used in the output filename.

    Returns:
        str: The filename of the saved CSV file with the constructed journal table.

    Example:
        >> result_file = construct_journal_table_task('api_data.json', 'example_api')
    """
    api_data = load_json(api_data_file)
    df = processing.construct_journal_table(api_data['results'])
    results_file_name = save_file(df, f"{api_name}_journal_df.csv", add_id=True)
    return results_file_name

@task()
def insert_in_db_task(df_files: dict, conn_params: dict) -> str:
    """
    Insert data from CSV files into a PostgreSQL database.

    Args:
        df_files (dict): A dictionary mapping table names to CSV file paths.
        postgres_conn_id (str): The connection ID for PostgreSQL.

    Example:
        >> df_files = {'paper': 'paper_df.csv', 'author': 'author_df.csv'}
        >> insert_in_db_task(df_files, 'my_postgres_conn')
    """
    df_dict = {}
    sql_dict = {}
    for key, file in df_files.items():
        df_dict[key] = pd.read_csv(file)
        with open(f'sql/insert_{key}.sql', 'r') as file:
            sql_dict[key] = file.read()
    
    databases.insert_in_db(df_dict, sql_dict, conn_params)


@task()
def get_connection_params_task() -> Dict[str, str]:
        conn_params = {
            "host": "postgres", 
            "database": "airflow", 
            "user": os.environ.get('POSTGRES_USER'), 
            "password": os.environ.get('POSTGRES_PASSWORD')}
        return conn_params

def create_pool_func(name: str, description: str, slots: int):
    try:
        _ = get_pool(name=name)
    except PoolNotFound:
        create_pool(name=name, slots=slots, description=description)
