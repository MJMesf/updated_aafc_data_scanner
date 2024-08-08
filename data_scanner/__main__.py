"""Parses AAFC's open data, both on the departmental AAFC Open Data Catalogue 
and on Canada's Open Government Portal, to provide the user with a complete 
inventory of datasets and resources, along with visualizations and statistics 
about all the published data.

NOTE: Problem accessing AAFC Open Data Catalogue, task left for later; 
current focus is on the Open Government Portal
"""

import atexit
import concurrent.futures
import datetime as dt
import pandas as pd
import re
import requests
import threading
import time
import urllib3

from dataclasses import dataclass
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
from typing import List, Any

from .inventory_functions import *


CATALOGUE_BASE_URL = 'https://data-catalogue-donnees.agr.gc.ca/api/3/action/'
"""Base url to send API requests to AAFC Open Data Catalogue"""
CATALOGUE_DATASETS_BASE_URL = \
    'https://data-catalogue-donnees.agr.gc.ca/aafc-open-data/{}'
"""Base url to open a dataset on the AAFC Open Data Catalogue, 
to format with its id
"""
CATALOGUE_RESOURCES_BASE_URL = \
    'https://data-catalogue-donnees.agr.gc.ca/aafc-open-data/{}/resource/{}'
"""Base url to open a resource on the AAFC Open Data Catalogue, 
to format with its package/datasets id, along with the resource id
"""

REGISTRY_BASE_URL = 'https://open.canada.ca/data/api/3/action/'
"""Base url to send API requests to open.canada.ca"""
REGISTRY_DATASETS_BASE_URL = \
    'https://open.canada.ca/data/en/dataset/{}'
"""Base url to open a dataset on the registry (open.canada.ca), 
to format with its id
"""
REGISTRY_RESOURCES_BASE_URL = \
    'https://open.canada.ca/data/en/dataset/{}/resource/{}'
"""Base url to open a resource on the registry (open.canada.ca), 
to format with its package/datasets id, along with the resource id
"""

AAFC_ORG_ID = '2ABCCA59-6C57-4886-99E7-85EC6C719218'
"""ID of the organization AAFC on the Open Registry"""

DATASETS_COLS = ['id', 'title_en', 'title_fr', 'published', 'modified',
                 'metadata_created', 'metadata_modified' , 
                 'num_resources', 'maintainer_email', 'maintainer_name',
                 'collection', 'frequency', 'currency', 'official_langs',
                 'registry_link', 'catalogue_link']
RESOURCES_COLS = ['id', 'title_en', 'title_fr', 'created', 
                  'metadata_modified', 'format', 'langs', 
                  'dataset_id', 'resource_type', 'url', 'url_status',
                  'registry_link', 'catalogue_link']

# Request Session Preparation (to avoid connection errors 5XX)
session = requests.Session()
retries = Retry(backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))

urllib3.disable_warnings()


@dataclass
class DataCatalogue:
    
    base_url: str
    """Base url of catalogue, to which API commands are appended"""

    def list_datasets(self) -> List[str]:
        """Returns list of all datasets (packages) IDs in the catalogue"""
        url: str = self.base_url + 'package_list'
        return request(url)
    
    def search_datasets(self, **kwargs: str) -> List[str]:
        """Returns IDs of datasets that match the given filters
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
        results: List[dict]

        # get all IDs 100 by 100
        while len(list_id) < count:
            filters = '+'.join(f'{key}:{val}' for key, val in kwargs.items())
            url = self.base_url + f'package_search?rows=100&start={i}&fq=' + filters
            results = request(url)['results']
            sublist_id = [dataset['id'] for dataset in results]
            list_id.extend(sublist_id)
            i += 100
        return list_id
    
    def get_dataset(self, id: str) -> dict:
        """Returns dataset's information, given its ID"""
        url: str = self.base_url + f'package_show?id={id}'
        return request(url)
    
    def get_resource(self, id: str) -> dict:
        """Returns resource's information, given its ID"""
        url: str = self.base_url + f'resource_show?id={id}'
        return request(url)



@atexit.register
def display_exit_message() -> None:
    """Displays a message when program ends."""
    print("\n---- Program ended.\n")



def get_and_retry(url):
    """Sends http request and retries in case of connection issues."""
    global session
    return session.get(url)

def get_head_and_retry(url):
    """Sends http request and retries in case of connection issues."""
    global session
    return session.head(url)

def request(url: str) -> Any:
    """Sends a CKAN API web request with a given URL and return the content 
    of the result
    """
    response: requests.models.Response
    # Pronlem accessing AAFC Open Data Catalogue; TO BE FIXED LATER
    if url.startswith('https://data-catalogue-donnees.agr.gc.ca/'):
        response = requests.get(url, verify=False)
    else:
        response = get_and_retry(url)
    assert response.status_code == 200, \
        f'Request Error:\nUnexpected status code: {response.status_code}'
    data = response.json()
    assert data['success'] == True, \
        f'CKAN API Error: request\'s success is False'
    return data['result']


def infer_name_from_email(email: str) -> str:
    """Infer name of the email owner from the given email address 
    (split and capitalize words before @).
    """
    name: str = ' '.join(re.split(r'[.\-_]', email.split('@')[0].lower()))
    return name.title()


def get_modified(ds: pd.Series, all_resources: pd.DataFrame) -> str:
    """Returns last date modified of the dataset ds, given the last 
    metadata_modified dates of its resources
    """
    last_modified: dt.datetime
    resources = all_resources[all_resources['dataset_id'] == ds['id']]
    modified_dates: List[dt.datetime] = []
    for _, res in resources.iterrows():
        modified_field = res['metadata_modified']
        if pd.notnull(modified_field):
            modified_date = dt.datetime.strptime(
                modified_field, '%Y-%m-%dT%H:%M:%S.%f')
            modified_dates.append(modified_date)
    if len(modified_dates) == 0:
        last_modified = dt.datetime.strptime(
            # dataset publication date by default if no metata_modified completed
            ds['published'], '%Y-%m-%dT%H:%M:%S')
    else:
        last_modified = max(modified_dates) # latest date
    return last_modified.isoformat()

def check_currency(ds: pd.Series, all_resources: pd.DataFrame,
                   now: dt.datetime = dt.datetime.now()) -> str:
    """Returns 'Up to date', 'Needs update' or 'Error reading frequency' 
    depending on the frequency and last date modified of the dataset.
    """
    
    # Computing oldest date considered as valid to be up to date
    frequency: str = ds['frequency']
    if frequency.startswith('P') and frequency != 'PT1S':
        duration: dt.timedelta
        unit: str = frequency[-1] # e.g. Y, M, W, D ...
        number: float = float(frequency[1:-1]) # e.g. 1, 2, 0.5 ...
        match unit:
            case 'Y':
                duration = dt.timedelta(days=number*365.25)
            case 'M':
                duration = dt.timedelta(days=number*30.43)
            case 'W':
                duration = dt.timedelta(weeks=number)
            case 'D':
                duration = dt.timedelta(days=number)
            case _:
                print(f'\nFrequency parsing Error in Dataset: {ds['id']}')
                print(f'Dataset info: {ds}\n')
                return 'Error reading frequency'
        oldest_valid_update: dt.datetime = now - duration
    else:
        return 'Up to date'

    # Getting last modified date, based on resources
    last_modified = dt.datetime.fromisoformat(ds['modified'])
    if last_modified >= oldest_valid_update:
        return 'Up to date'
    else:
        return 'Needs update'

def main() -> None:
    
    # Problem accessing AAFC's Open Data Catalogue; TO BE FIXED LATER !!!
    # aafc_odc = DataCatalogue(AAFC_ODC_BASE_URL)
    registry = DataCatalogue(REGISTRY_BASE_URL)

    # Creating datasets inventory
    datasets_IDs: List[str]
    datasets = pd.DataFrame(columns=DATASETS_COLS)

    # Creating resources inventory
    resources_IDs: List[str] = []
    resources = pd.DataFrame(columns=RESOURCES_COLS)


    # Collecting information of all datasets published by AAFC:

    print()
    print('Collecting information of all datasets ...')
    start = time.time() # times datasets collection

    # listing all the datasets IDs:
    datasets_IDs = registry.search_datasets(owner_org=AAFC_ORG_ID)
    # initializing the progress bar
    pbar1 = tqdm(desc='Processed Datasets', total=len(datasets_IDs), 
                 colour='green', ncols=100, ascii=' =')

    # in parallel threads, collects the datasets relevant information
    # and lists their resources IDs
    datasets_lock = threading.Lock()
    resources_IDs_lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for id in datasets_IDs:
            # add dataset to the common dataframe and lists its resources IDs 
            executor.submit(collect_dataset, registry, id,
                            datasets, datasets_lock,
                            resources_IDs, resources_IDs_lock,
                            pbar1)
    pbar1.close()
    end = time.time() # ends datasets collection timer

    datasets.sort_values(by='published', ascending=False, inplace=True)
    datasets.reset_index(drop=True, inplace=True)
    print(f'-- All {len(datasets_IDs)} datasets\' information was collected.')
    print(f'   {len(resources_IDs)} associated resources were listed.  ({end-start:.2f}s)')


    # Collecting information of all associated resources:

    print()
    print('Collecting information of all the resources ...')
    start = time.time() # times resources collection

    # initializing the progress bar
    pbar2 = tqdm(desc='Processed Resources', total=len(resources_IDs), 
                 colour='green', ncols=100, ascii=' =')

    # in parallel threads, collects the resources relevant information:
    resources_lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for id in resources_IDs:
            # add resources to the common dataframe
            executor.submit(collect_resource, registry, id,
                            resources, resources_lock, 
                            pbar2)
    pbar2.close()
    end = time.time() # end of resources collection

    resources.sort_values(by='dataset_id', inplace=True)
    resources.reset_index(drop=True, inplace=True)
    print(f'-- All {len(resources)} resources\' information was collected.  ({end-start:.2f}s)')


    # Computes missing fields of datasets inventories

    # Compute modified for datasets
    print()
    print('Completing "modified" column of the datasets')
    datasets['modified'] = datasets.apply(lambda x: get_modified(x, resources), axis=1)
    # Compute currency for datasets
    print("Computing datasets' currency (i.e. checking if datasets are up to date).")
    datasets['currency'] = datasets.apply(lambda x: check_currency(x, resources), axis=1)
    # Complete official_langs
    pass            # to implement later
    print("Inventories are ready.")

    # Exporting inventories
    
    print()
    timestamp: str = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename1: str = f'./../inventories/datasets_inventory_{timestamp}.csv'
    datasets.to_csv(filename1, index=False, encoding='utf_8_sig')
    print(f'Datasets inventory was successfully exported to {filename1}.')

    filename2: str = f'./../inventories/resources_inventory_{timestamp}.csv'
    resources.to_csv(filename2, index=False, encoding='utf_8_sig')
    print(f'Resources inventory was successfully exported to {filename2}.')


if __name__ == '__main__':
    main()