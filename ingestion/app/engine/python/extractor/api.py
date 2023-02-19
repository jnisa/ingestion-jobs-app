# Script that contains all the ingestion related functions

import requests


from ingestion.app.engine.python.utils.configs import API_KEY, BASE_URL



def get_jobs_request(keyword: str = 'data engineering', page_number: int = 1):
    """
    Perform a request to get all the jobs from the USAJOBS API. This function also
    gets a keyword if you need to be more specific within your search procedures and 
    the results page number you want to get in return.

    :param keyword: search keyword that will filter the job results from the API
    :param page_number: page number that you want to search for
    :return: not only a list of all the jobs that encounter the keyword provided and the page
    number provided to this function but it also returns the number of pages that this 
    search contains. 
    """

    attributes = [f'Keyword={keyword}', f'Page={str(page_number)}']
    url = BASE_URL + '&'.join(attributes)
    headers = {'Authorization-Key': API_KEY}

    try: 
        response = requests.get(url, headers=headers, timeout = 10)
    except:
        raise Exception("Request timed out")

    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}")
    else:
        data = response.json()['SearchResult']['SearchResultItems']
        pages_number = response.json()['SearchResult']['UserArea']['NumberOfPages']

    return data, pages_number
