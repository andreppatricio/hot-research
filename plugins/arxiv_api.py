import urllib.request as libreq
import urllib
import feedparser
from datetime import datetime
import time
import pandas as pd
from typing import Dict


# Base api query url
API_NAME = 'arxiv'
BASE_URL = 'http://export.arxiv.org/api/query?'

def request_api(url_str: str) -> feedparser.FeedParserDict:
    """
    Send a request to the specified API URL and parse the response.

    Args:
        url_str (str): The URL to send the request to.

    Returns:
        feedparser.FeedParserDict: Parsed response from the API.

    Raises:
        Exception: If the response code is not 200 or if the API returns empty results.

    Example:
        >> url = 'http://export.arxiv.org/api/query?search_query=all:arxiv&start=50&max_results=1&sortBy=submittedDate&sortOrder=descending' 
        >> response = request_api(url)
        >> response.entries[0]['title']
        'Example Title'
    """
    print(url_str)
    with libreq.urlopen(url_str) as url:
        response = url.read()
        response_code = url.getcode()
    feed = feedparser.parse(response)
    # print(response_code)
    # print(feed.entries)

    if response_code != 200:  # Not successful
        msg = feed.entries[0].summary if len(feed.entries) == 1  else "No message"
        raise Exception(response_code, msg)
    elif len(feed.entries) == 0:
        raise Exception("API returned empty results")
    else:
        return feed


def query_api(search_query: str, start_date, end_date, results_per_call=100, sleep_time=3) -> dict:
    """
    Query the specified API for data within a date range.

    Args:
        search_query (str): The search query for the API.
        start_date (str): The start date of the date range in the format '%Y-%m-%d'.
        end_date (str): The end date of the date range in the format '%Y-%m-%d'.
        results_per_call (int, optional): Number of results per API call. Defaults to 100.
        sleep_time (int, optional): Sleep time between API calls in seconds. Defaults to 3.

    Returns:
        dict: JSON-like dictionary containing the results.

    Example:
        >> dates = {'start_date': '2023-11-06', 'end_date': '2023-11-08'}
        >> search_query = 'all:arxiv'
        >> response = query_api(search_query, dates['start_date'], dates['end_date'], results_per_call=50, sleep_time=2)
        >> set(response.keys()) == {'results'}
        >> response['results'][0]
        {'id': '2311.05052v1', 'publishedDate': '2023-11-08T23:02:23Z', 'title': 'Matrix Completion via Memoryless Scalar Quantization', \
            'doi': None, 'publisher': None, 'identifiers': [], 'journals': [], 'authors': ['Arian Eamaz', 'Farhang Yeganegi', 'Mojtaba Soltanalian']}
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    print(f"Start date: {start_date} | End date: {end_date}")
    # iter_date = datetime.now()
    date_format = "%Y-%m-%dT%H:%M:%SZ"

    ### Find parameter {start} for query corresponding to the desired interval
    start = 0
    while True:
        print("\n", '*'*10)
        query = f'search_query={search_query}&start={start + results_per_call}&max_results={1}&sortBy=submittedDate&sortOrder=descending'
        feed = request_api(BASE_URL+query)
        date = datetime.strptime(feed.entries[0].published, date_format).date()
        print(f"start={start + results_per_call} returns date {date}")
        print(f"Is {date}  <=  {end_date} ? --> {date <= end_date}")
        if date <= end_date:
            break
        else:
            start += results_per_call

    print('\nSelected Start: ', start)

    papers = []
    keep_going = True
    while keep_going:
        print('*'*10)
        print(f"Results for {start} - {start + results_per_call}")

        query = f'search_query={search_query}&start={start}&max_results={results_per_call}&sortBy=submittedDate&sortOrder=descending'
        # perform a GET request using the base_url and query
        feed = request_api(BASE_URL+query)
        # DEBUG
        d = f'start: {datetime.strptime(feed.entries[-1].published, date_format)} | end: {datetime.strptime(feed.entries[0].published, date_format)}'
        print(f'\n{d}\n')
        # DEBUG

        # Run through each entry, and print out information
        for entry in feed.entries:
            pdate = datetime.strptime(entry.published, date_format).date()
            if pdate > end_date:
                continue
            elif pdate < start_date:
                keep_going = False
                break
            paper = {}
            paper['id'] = entry.id.split('/abs/')[-1]
            paper["publishedDate"] = entry.published
            paper["title"] = entry.title
            paper["doi"] = None
            paper["publisher"] = None
            paper["identifiers"] = []
            paper['journals'] = []
            authors = []
            for author in entry.authors:
                authors.append(author['name'])
            paper['authors'] = authors
            papers.append(paper)
        start += results_per_call

        time.sleep(sleep_time)

    results_json = {'results': papers}
    return results_json


def get_api_data(dates: Dict[str, str]) -> list:
    """
    Get data from the API within the specified date range.

    Args:
        dates (dict): A dictionary containing start_date and end_date for the date range.

    Returns:
        list: A list of dictionaries containing API data.

    Example:
        >> dates = {'start_date': '2023-11-06', 'end_date': '2023-11-08'}
        >> api_data = get_api_data(dates)
        >> api_data
        {'id': '2311.05052v1', 'publishedDate': '2023-11-08T23:02:23Z', 'title': 'Matrix Completion via Memoryless Scalar Quantization',\
              'doi': None, 'publisher': None, 'identifiers': [], 'journals': [], 'authors': ['Arian Eamaz', 'Farhang Yeganegi', 'Mojtaba Soltanalian']}
    """
    start_date, end_date = dates['start_date'], dates['end_date']
    print(f"Getting data for data interval: {start_date} - {end_date}")
    search_query = urllib.parse.quote("all:arxiv")
    response = query_api(search_query, start_date, end_date, results_per_call=1000, sleep_time=3)
    print(len(response['results']))
    return response['results']