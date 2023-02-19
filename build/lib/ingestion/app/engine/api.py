# Script that contains all the ingestion related functions

import pdb
import requests


from app.utils.configs import BASE_URL



def get_jobs_request(keyword: str = 'data engineering', page_number: int = 1):
    """
    Perform a request to get all the jobs from the USAJOBS API. This function also
    gets a keyword if you need to be more specific within your search procedures and 
    the results page number you want to get in return.

    :param keyword: search keyword that will filter the job results from the API
    :param pages_number: page number that you want to search for
    :return: not only a list of all the jobs that encounter the keyword provided and the page
    number provided to this function but it also returns the number of pages that this 
    search contains. 
    """

    attributes = [f'Keyword={keyword}', f'Page={str(page_number)}']
    # TODO. this needs to be in a Secrets Manager
    headers = {'Authorization-Key': '9MbHa87/i38Y36f7BjnF/HnyPGEsOUWXJdInME0B99E='}

    url = BASE_URL + '&'.join(attributes)
    response = requests.get(url, headers=headers)

    data = response.json()['SearchResult']['SearchResultItems']
    pages_number = response.json()['SearchResult']['UserArea']['NumberOfPages']

    return data, pages_number


data, number = get_jobs_request()
pdb.set_trace()