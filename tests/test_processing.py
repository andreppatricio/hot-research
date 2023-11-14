import os
import sys
# Get the path to the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
# Add the parent directory to the Python path
sys.path.append(parent_dir)

import pytest
import pandas as pd
import nltk
# from unittest.mock import patch, MagicMock
import plugins
from plugins.processing import (
    filter_english_titles,
    construct_paper_table,
    construct_author_table,
    construct_journal_table,
    extract_ngram_keywords,
)


def test_filter_english_titles():
    api_data = {'results': [
        {'title': 'English Title 1'},
        {'title': 'Titlo em Portugues'},
        {'title': 'English Title 2'}
    ]}

    result = filter_english_titles(api_data)

    expected_result = {'results': [{'title': 'English Title 1'}, {'title': 'English Title 2'}]}
    assert result == expected_result



def test_construct_paper_table():
    api_data = [{'id': 1, 'title': 'Paper 1', 'doi': 'doi1', 'publishedDate': '2023-01-01', 'publisher': 'Publisher 1'}]
    result = construct_paper_table(api_data)

    expected_columns = ['id', 'title', 'doi', 'published_date', 'publisher']
    assert result.columns.tolist() == expected_columns
    assert result.shape == (1, len(expected_columns))


def test_construct_author_table():
    api_data = [{'id': 1, 'authors': ['Author 1', 'Author 2']}]
    result = construct_author_table(api_data)

    expected_columns = ['paper_id', 'name']
    assert result.columns.tolist() == expected_columns
    assert result['name'].tolist() == ['Author 1', 'Author 2']



def test_construct_journal_table():
    api_data = [{'id': 1, 'journals': [{'identifiers': ['issn:123456'], 'title': 'Journal 1'}]}]
    result = construct_journal_table(api_data)

    expected_columns = ['paper_id', 'issn', 'name']
    assert result.columns.tolist() == expected_columns
    assert result['name'].tolist() == ['Journal 1']


def test_extract_ngram_keywords(mocker):
    titles = ['Title about something and something else', 'Title about that and this and something']

    mocker.patch('plugins.processing.get_nltk_stopwords', return_value=['and', 'this', 'that'])

    result1 = extract_ngram_keywords(titles, 1)
    result2 = extract_ngram_keywords(titles, 2)

    expected_result1 = {'top_words_dict': {'about': 2, 'else': 1, 'something': 3, 'title': 2}, 'total_week_count': 8}
    assert result1 == expected_result1
    expected_result2 = {'top_words_dict': {'about something': 1, 'something else': 1, 'title about': 2}, 'total_week_count': 4}
    assert result2 == expected_result2