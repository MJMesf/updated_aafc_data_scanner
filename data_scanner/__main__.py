"""Parses AAFC's open data, both on the departmental AAFC Open Data Catalogue 
and on Canada's Open Government Portal, to provide the user with a complete 
inventory of datasets and resources, along with visualizations and statistics 
about all the published data.

NOTE: Problem accessing AAFC Open Data Catalogue, task left for later; 
current focus is on the Open Government Portal
"""

import concurrent.futures
import datetime as dt
import pandas as pd
import requests
import threading
import time
import urllib3

from dataclasses import dataclass
from typing import List, Dict, Any

AAFC_ODC_BASE_URL = 'https://data-catalogue-donnees.agr.gc.ca/api/action/'
"""Base url to send API requests to AAFC Open Data Catalogue"""
REGISTRY_BASE_URL = 'https://open.canada.ca/data/api/action/'
"""Base url to send API requests to open.canada.ca"""
AAFC_ORG_ID = '2ABCCA59-6C57-4886-99E7-85EC6C719218'
"""ID of the organization AAFC on the Open Registry"""

@dataclass
class DataCatalogue:
    
    base_url: str
    """Base url of catalogue, to which API commands are appended"""

    def list_datasets(self) -> List[str]:
        """Returns list of all datasets (packages) IDs in the catalogue"""
        url: str = self.base_url + 'package_list'
        return request(url)
    
    def search_datasets(self, **kwargs: str) -> List[str]:
        """Returns ids of datasets that match the given filters
        e.g. groups='test-group'
        """
        filters: str = '+'.join(f'{key}:{val}' for key, val in kwargs.items())
        url: str = self.base_url + 'package_search?fq=' + filters
        # checks total number of results
        count: int = request(url)['count']
        
        # creates a list to be filled with datasets' IDs
        list_id: List[str] = []
        i: int = 0
        sublist_id: List[str]

        # get all IDs 100 by 100
        while len(list_id) < count:
            filters = '+'.join(f'{key}:{val}' for key, val in kwargs.items())
            url = self.base_url + f'package_search?rows=100&start={i}&fq=' + filters
            results: List[Dict[str, Any]] = request(url)['results']
            sublist_id = [dataset['id'] for dataset in results]
            list_id.extend(sublist_id)
            i += 100
        return list_id
    
    def get_dataset(self, id: str) -> Dict[str, Any]:
        """Returns dataset's information, given its ID"""
        url: str = self.base_url + f'package_show?id={id}'
        return request(url)


def request(url: str) -> Any:
    """Sends an CKAN API web request with a given URL and return the content 
    of the result
    """
    response: requests.models.Response
    # Pronlem accessing AAFC Open Data Catalogue; TO BE FIXED LATER
    if url.startswith('https://data-catalogue-donnees.agr.gc.ca/'):
        urllib3.disable_warnings()
        response = requests.get(url, verify=False)
    else:
        response = requests.get(url)
    assert response.status_code == 200, \
        f'Request Error:\nUnexpected status code: {response.status_code}'
    data = response.json()
    assert data['success'] == True, \
        f'CKAN API Error: request\'s success is False'
    return data['result']

def add_dataset(catalogue: DataCatalogue, id: str, 
                datasets: pd.DataFrame, index: int,
                lock: threading.Lock) -> None:
    """Fetches the dataset's information from the catalog with its id, then 
    enters its information in the datasets at the given index.
    """
    dataset = catalogue.get_dataset(id)
    record: Dict[str, Any] = {
        'id': dataset['id'],
        'title_en': dataset['title_translated']['en'],
        'title_fr': dataset['title_translated']['fr'],
        'date_published': dataset['date_published'],
        'metadata_created': dataset['metadata_created'],
        'metadata_modified': dataset['metadata_modified'],
        'num_resources': dataset['num_resources'],
        'author_email': dataset['author_email'],
        'maintainer_email': dataset['maintainer_email']
    }
    lock.acquire()
    datasets.loc[index] = record
    lock.release()


def main():
    
    # Problem accessing AAFC's Open Data Catalogue; TO BE FIXED LATER
    # aafc_odc = DataCatalogue(AAFC_ODC_BASE_URL)
    registry = DataCatalogue(REGISTRY_BASE_URL)

    # Creating datasets inventory
    datasets_cols = ['id', 'title_en', 'title_fr', 'date_published', 
                     'metadata_created', 'metadata_modified' , 
                     'num_resources', 'author_email', 'maintainer_email']
    datasets = pd.DataFrame(columns=datasets_cols)

    # Collecting information of all datasets published by AAFC:

    print()
    print('Collecting information of all datasets ...')
    start = time.time()
    # listing the IDs:
    IDs = registry.search_datasets(owner_org=AAFC_ORG_ID)
    lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(len(IDs)):
            executor.submit(add_dataset, registry, IDs[i], datasets, i, lock)
    end = time.time()
    datasets.reset_index(drop=True, inplace=True)
    print(f'-- All {len(IDs)} datasets information was collected ({end-start:.2f}s)')
    print()

    # saving inventory to an excel file
    timestamp: str = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename: str = f'datasets_inventory_{timestamp}.xlsx'
    datasets.to_excel(filename, index=False)


if __name__ == '__main__':
    main()