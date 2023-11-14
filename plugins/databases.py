import psycopg2
import pandas as pd
from typing import Dict


def create_tables(conn_params: Dict[str, str]):
    with open('sql/create_tables.sql', 'r') as file:
        sql_create_tables = file.read()

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_create_tables)
    conn.close()


def create_schema(conn_params: Dict[str, str]):
    with open('sql/create_schema_papers.sql', 'r') as file:
        sql_create_schema = file.read()

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_create_schema)
    conn.close()


def insert_in_db(df_dict: Dict[str, pd.DataFrame], sql_dict: Dict[str, str], conn_params: Dict[str, str]):
    """
    Insert data from DataFrames into a PostgreSQL database.

    Args:
        df_dict (dict): A dictionary mapping table names to DataFrames.
        sql_dict (dict): A dictionary mapping table names to SQL insert statements.
        postgres_conn_id (str): The connection ID for PostgreSQL.

    Example:
        >> df_paper = pd.DataFrame({'id': [1], 'title': ['Paper 1'], 'doi': ['doi1'], 'published_date': ['2023-01-01'], 'publisher': ['Publisher 1']})
        >> df_author = pd.DataFrame({'paper_id': [1], 'name': ['Author 1']})
        >> df_journal = pd.DataFrame({'paper_id': [1], 'issn': ['123456'], 'name': ['Journal 1']})
        >> df_dict = {'paper': df_paper, 'author': df_author, 'journal': df_journal}
        >> sql_dict = {'paper': 'INSERT INTO papers.paper ...', 'author': 'INSERT INTO papers.author ...', 'journal': 'INSERT INTO papers.journal ...'}
        >> insert_in_db(df_dict, sql_dict, 'my_postgres_conn')
    """
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    # Iterate through the DataFrame rows and insert them into the database
    df_paper = df_dict['paper']
    df_author = df_dict['author']
    df_journal = df_dict['journal']

    for index, row in df_paper.iterrows():
        paper_data = row.to_dict()
        paper_data.update({'api_name': 'core'})
        try:
            with conn:
                with conn.cursor() as cursor:
                    # Begin transaction
                    # Insert into papers.paper
                    cursor.execute(sql_dict['paper'], paper_data)
                    # Insert into papers.author
                    for i, author_row in df_author[df_author["paper_id"] == row["id"]].iterrows():
                        author_data = author_row.to_dict()
                        author_data.update({'api_name': 'core'})
                        cursor.execute(sql_dict['author'], author_data)
                    # Insert into papers.journal
                    for i, journal_row in df_journal[df_journal["paper_id"] == row["id"]].iterrows():
                        journal_data = journal_row.to_dict()
                        journal_data.update({'api_name': 'core'})
                        cursor.execute(sql_dict['journal'], journal_data)

        except psycopg2.IntegrityError as e:
            print("IntegrityError:", e)
            conn.rollback()
    conn.close()


def get_titles_from_db(dates: Dict[str, str], conn_params: dict):
    """
    Retrieve titles from the database based on a date range.

    Args:
        dates (dict): A dictionary containing start_date and end_date for the date range.
        postgres_conn_id (str): The connection ID for PostgreSQL.

    Returns:
        list: A list of titles.

    Example:
        >> dates = {'start_date': '2023-01-01', 'end_date': '2023-12-31'}
        >> titles = get_titles_from_db(dates, 'my_postgres_conn')
        >> len(titles)
        1
    """
   
    sql_get_titles = """
            SELECT title FROM papers.paper WHERE published_date BETWEEN %(start_date)s AND %(end_date)s;
        """
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_get_titles, dates)
            results = cursor.fetchall()
    conn.close()

    results = [r[0] for r in results]
    return results


def insert_keywords_in_db(keywords_dict: dict, dates: Dict[str, str], n: int, conn_params: Dict[str, str]):
    """
    Insert keyword data into the database.

    Args:
        keywords_dict (dict): A dictionary containing keyword information.
        dates (dict): A dictionary containing start_date and end_date for the date range.
        n (int): The size of the n-grams.
        postgres_conn_id (str): The connection ID for PostgreSQL.

    Example:
        >> keywords_dict = {'top_words_dict': {'Keyword1': 10, 'Keyword2': 5}, 'total_word_count': 15}
        >> dates = {'start_date': '2023-01-01', 'end_date': '2023-01-07'}
        >> insert_keywords_in_db(keywords_dict, dates, 2, 'my_postgres_conn')
    """
    
    week_start = dates['start_date']
    sql_keyword_insert = """
        INSERT INTO papers.keyword (week_start_date, n, word, count, week_percentage) 
        VALUES (%(week_start_date)s, %(n)s, %(word)s, %(count)s, %(week_percentage)s);
    """

    top_words_dict = keywords_dict["top_words_dict"]
    total_week_count = keywords_dict["total_week_count"]

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    # Iterate through the DataFrame rows and insert them into the database
    for keyword, count in top_words_dict.items():
        keyword_data = {'week_start_date': week_start, 'n': n, 'word': keyword, 'count': count, 'week_percentage': round(100*count/total_week_count, 5)}
        try:
            with conn:
                with conn.cursor() as cursor:
                    # Begin transaction
                    cursor.execute(sql_keyword_insert, keyword_data)

        except psycopg2.IntegrityError as e:
            print("IntegrityError:", e)
            conn.rollback()
    conn.close()

