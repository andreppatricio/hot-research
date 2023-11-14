from typing import List
import langdetect
import pandas as pd
import nltk
from collections import Counter

def filter_english_titles(api_data: dict) -> dict:
    """
    Filter English titles from API data.

    Args:
        api_data (dict): The API data containing 'results' key.

    Returns:
        dict: The API data with filtered results.

    Example:
        >> api_data = {'results': [{'title': 'English Title 1'}, {'title': 'Titlo em Portugues'}, {'title': 'English Title 2'}]}
        >> filtered_data = filter_english_titles(api_data)
        >> filtered_data
        {'results': [{'title': 'English Title 1'}, {'title': 'English Title 2'}]}
    """
    # bsize = len(api_data['results'])
    filtered_results = []
    for result in api_data['results']:
        try:
            if langdetect.detect(result['title']) == 'en':
                filtered_results.append(result)
        except langdetect.detector.LangDetectException: # type: ignore
            continue

    api_data['results'] = filtered_results
    return api_data


def construct_paper_table(api_data: List[dict]) -> pd.DataFrame:
    """
    Construct a pandas DataFrame for paper data from API data.

    Args:
        api_data (list): The API data containing paper information.

    Returns:
        pd.DataFrame: The constructed DataFrame.

    Example:
        >> api_data = [{'id': 1, 'title': 'Paper 1', 'doi': 'doi1', 'publishedDate': '2023-01-01', 'publisher': 'Publisher 1'}]
        >> df = construct_paper_table(api_data)
        >> df.columns.tolist() == ['id', 'title', 'doi', 'published_date', 'publisher'] and df.shape == (1, 5)
        True
    """
    def extract_doi(result: dict):
        doi = None
        if result['doi'] is not None:
            doi = result['doi']
        elif 'identifiers' in result.keys():
            identifiers = result['identifiers']
            for id in identifiers:
                if id['type'] == "DOI":
                    doi = id['identifier']
        else:
            doi = None
        return doi

    df = pd.DataFrame(columns=['id', 'title', 'doi', 'published_date', 'publisher'])
    rows_list = []
    for res in api_data:
        paper_row = {}
        paper_row['id'] = res['id']
        paper_row['title'] = res['title']
        paper_row['doi'] = extract_doi(res) 
        paper_row['published_date'] = res['publishedDate']
        paper_row['publisher'] = res['publisher']
        rows_list.append(paper_row)
    df = pd.concat([df, pd.DataFrame(rows_list)])
    return df


def construct_author_table(api_data: List[dict]) -> pd.DataFrame:
    """
    Construct a pandas DataFrame for author data from API data.

    Args:
        api_data (list): The API data containing author information.

    Returns:
        pd.DataFrame: The constructed DataFrame.

    Example:
        >> api_data = [{'id': 1, 'authors': ['Author 1', 'Author 2']}]
        >> df = construct_author_table(api_data)
        >> df['name'].tolist() == ['Author 1', 'Author 2'] and df.columns.tolist() == ['paper_id', 'name']
        True
    """
    df = pd.DataFrame(columns=['paper_id', 'name'])
    rows_list = []
    for res in api_data:
        for name in res['authors']:
            author_row = {}
            author_row['paper_id'] = res['id']
            author_row['name'] = name
            rows_list.append(author_row)
    df = pd.concat([df, pd.DataFrame(rows_list)])
    return df


def construct_journal_table(api_data: List[dict]) -> pd.DataFrame:
    """
    Construct a pandas DataFrame for journal data from API data.

    Args:
        api_data (list): The API data containing journal information.

    Returns:
        pd.DataFrame: The constructed DataFrame.

    Example:
        >> api_data = [{'id': 1, 'journals': [{'identifiers': ['issn:123456'], 'title': 'Journal 1'}]}]
        >> df = construct_journal_table(api_data)
        >> df['name'].tolist() == ['Journal 1'] and df.columns.tolist() == ['paper_id', 'issn', 'name']
        True
    """
    def extract_issn(journal: dict):
        issn = None
        identifiers = journal['identifiers']
        for id in identifiers:
            if 'issn' in id:
                issn = id.split(':')[1]
        return issn

    df = pd.DataFrame(columns=['paper_id', 'issn', 'name'])
    rows_list = []

    for res in api_data:
        for journal in res['journals']:
            journal_row = {}
            journal_row['paper_id'] = res['id']
            journal_row['issn'] = extract_issn(journal)
            journal_row['name'] = journal['title']
            rows_list.append(journal_row)
    df = pd.concat([df, pd.DataFrame(rows_list)])
    return df


def get_nltk_stopwords(language: str):
    try:
        words = nltk.corpus.stopwords.words(language)
    except LookupError:
        nltk.download('stopwords')
        words = nltk.corpus.stopwords.words(language)
    return words



def extract_ngram_keywords(titles: List[str], n: int, n_top_keywords=100) -> dict:
    """
    Extract n-gram keywords from a list of titles.

    Args:
        titles (list): List of titles.
        n (int): The size of the n-grams.
        n_top_keywords (int, optional): Number of top keywords to return. Defaults to 100.

    Returns:
        dict: A dictionary containing the top n-gram keywords and the total word count.

    Example:
        >> titles = ['Title about something and something else', 'Title about that and this and something']
        >> keywords_dict1 = extract_ngram_keywords(titles, 1)
        >> keywords_dict2 = extract_ngram_keywords(titles, 2)
        >> keywords_dict1 == {'top_words_dict': {'something': 3, 'title': 2, 'else': 1}, 'total_week_count': 6} and \
            keywords_dict2 == {'top_words_dict': {'something else': 1}, 'total_week_count': 1}
        True
        
    """
    nltk.download('stopwords')
    nltk.download('punkt')
    languages = ["english", "spanish", "italian", "french", "german", "hungarian",
                "indonesian", "portuguese", "russian", "arabic"]
    stop_words = set()
    for language in languages:
        stop_words = stop_words.union(set(get_nltk_stopwords(language)))

    # Initialize a list to hold all words
    all_keywords = []

    # Tokenize each phrase into words and remove stopwords
    for title in titles:
        # Filter for alphabetic characthers, white space, and hyphen
        title = title.replace('-', ' ')
        only_alphabetic_title = ''.join(e.lower() for e in title if e.isalpha() or e==' ')
        word_list_title = only_alphabetic_title.split()
        my_ngrams = nltk.ngrams(word_list_title, n)
        for ngram in my_ngrams:
            stopword_flag = False
            for word in ngram:
                if word in stop_words:
                    stopword_flag = True
                    break
            if not stopword_flag:
                all_keywords.append(" ".join(ngram))

    # Count the occurrences of each word
    ngram_counts = Counter(all_keywords)
    top_ngrams_dict = dict(ngram_counts.most_common(n_top_keywords))
    total_week_count = sum(dict(ngram_counts).values())
    keywords_dict = {'top_words_dict': top_ngrams_dict, 'total_week_count': total_week_count}

    return keywords_dict
    