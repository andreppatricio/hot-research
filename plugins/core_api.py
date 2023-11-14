import requests
import time
from typing import Callable, List, Tuple

# TODO: hide credentials
CORE_API_KEY = "UsE6ibdfpg29GtwDLThxZVB51Cl0WNvm"

api_endpoint = "https://api.core.ac.uk/v3/"


def query_api(search_url: str, query: str, scrollId = None, limit=5000) -> Tuple[dict, float]:
    """
    Query the CORE API with the specified parameters.

    Args:
        search_url (str): The API endpoint URL.
        query (str): The search query for the API.
        scrollId (str, optional): The scroll ID for pagination (multiple calls). Defaults to None.
        limit (int, optional): The maximum number of results per query. Defaults to 7000.

    Returns:
        tuple: A tuple containing the JSON response (dict) and the elapsed time (float).

    Raises:
        RuntimeError: If the API response status code is not 200.

    Example:
        # doctest: +SKIP 
        >> search_url = "https://api.core.ac.uk/v3/search/works"
        >> query = 'publishedDate>=2023-11-06 AND publishedDate<=2023-11-08'
        >> result, elapsed_time = query_api(search_url, query, limit=100)
        >> set(result.keys()) == {'totalHits', 'limit', 'offset', 'scrollId', 'results', 'tooks', 'esTook'} and len(result['results']) > 0
        True
    """

    headers={"Authorization":"Bearer "+CORE_API_KEY}
    if not scrollId:
        response = requests.get(f"{search_url}?q={query}&limit={limit}&scroll=true",headers=headers)
    else:
        response = requests.get(f"{search_url}?q={query}&limit={limit}&scrollId={scrollId}",headers=headers)        
    
    if response.status_code == 200:
        return response.json(), response.elapsed.total_seconds()
    else:
        raise RuntimeError(response, response.text, response.content)
 

def scroll(url_fragment: str, query: str, extract_info_callback: Callable, sleep=10) -> List:
    """
    Scroll through the results of the CORE API using pagination.

    Args:
        url_fragment (str): The API endpoint URL fragment.
        query (str): The search query for the API.
        extract_info_callback (callable): A callback function to extract information from each API result.
        sleep (int, optional): Sleep time between API calls in seconds. Defaults to 10.

    Returns:
        list: A list with paper's information from the API.

    Example:
        >> url_fragment = "search/works"
        >> query = 'publishedDate>=2023-11-06 AND publishedDate<=2023-11-08'
        >> callback = lambda hit: {'id': hit['id'], 'title': hit['title']}
        >> results = scroll(url_fragment, query, callback, sleep=5) 
        >> results[0] == {'id': 133718706, 'title': 'Mary, the Holy Mother of God/World Day of Peace - 1 January 2023'}
        True
    """
    search_url = api_endpoint + url_fragment
    allresults = []
    count = 0
    scrollId=None
    while True:
        result, elapsed =query_api(search_url, query, scrollId)
        scrollId=result["scrollId"]
        totalhits = result["totalHits"]
        result_size = len(result["results"])
        if result_size==0:
            break
        for hit in result["results"]:
            info = extract_info_callback(hit)
            if info is not None:
                allresults.append(info)
        count+=result_size
        # print(f"{count}/{totalhits} {elapsed}s")
        time.sleep(sleep)
    return allresults


def get_api_data(dates: dict) -> List[dict]:
    """
    Get data from the CORE API within the specified date range.

    Args:
        dates (dict): A dictionary containing start_date and end_date for the date range.

    Returns:
        list: A list of dictionaries containing API data.

    Example:
        >> dates = {'start_date': '2023-11-06', 'end_date': '2023-11-08'}
        >> api_data = get_api_data(dates)
        >> set(api_data[0].keys()) == {'identifiers', 'publisher', 'journals', 'authors', 'doi', 'id', 'title', 'publishedDate'}
        True
        >> api_data[0]
        {'authors': ['Gemmell, Patricia'], 'doi': None, 'id': 133718706, 'identifiers': [{'identifier': 'oai:researchonline.nd.edu.au:pastoral-liturgy-1165', 'type': 'OAI_ID'},\
                'title': 'Mary, the Holy Mother of God/World Day of Peace - 1 January 2023', 'publishedDate': '2023-11-07T08:00:00', 'publisher': 'ResearchOnline@ND', 'journals': []}

    """
    def get_table_cols(hit):
        if hit['language'] is None or hit['language']['code'] == 'en':
            fields_to_keep = ['id', 'title', 'doi', 'publishedDate', 'publisher', 'authors', 'journals', 'identifiers']
            result = {k:v for k,v in hit.items() if k in fields_to_keep}
            result['authors'] = [r['name'] for r in result['authors']]
        else:
            result = None
        return result
    
    # Query API
    start_date, end_date = dates['start_date'], dates['end_date']
    print(f"Getting CORE data for date interval: {start_date} - {end_date}")

    date_variable = 'publishedDate'
    query = f"{date_variable}>={start_date} AND {date_variable}<={end_date}"
    url_fragment = "search/works"
    results = scroll(url_fragment, query, get_table_cols, sleep=10)
    return results


