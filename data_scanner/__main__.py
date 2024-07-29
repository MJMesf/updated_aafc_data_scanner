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
import requests
import threading
import time
import urllib3

from dataclasses import dataclass
from tqdm import tqdm
from typing import List, Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential


AAFC_ODC_BASE_URL = 'https://data-catalogue-donnees.agr.gc.ca/api/3/action/'
"""Base url to send API requests to AAFC Open Data Catalogue"""
REGISTRY_BASE_URL = 'https://open.canada.ca/data/api/3/action/'
"""Base url to send API requests to open.canada.ca"""
AAFC_ORG_ID = '2ABCCA59-6C57-4886-99E7-85EC6C719218'
"""ID of the organization AAFC on the Open Registry"""
DATASETS_COLS = ['id', 'title_en', 'title_fr', 'date_published', 
                 'metadata_created', 'metadata_modified' , 
                 'num_resources', 'maintainer_email', 'collection',
                 'frequency', 'needs_update']
RESOURCES_COLS = ['id', 'title_en', 'title_fr', 
                  'created', 'metadata_modified', 'format', 'en', 'fr', 
                  'package_id', 'resource_type', 'url', 'url_status']
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


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=1, max=20)
)
def get_and_retry(url):
    """Sends http request and retries in case of connection issues."""
    return requests.get(url)

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


def add_dataset(dataset: dict, datasets: pd.DataFrame,
                lock: threading.Lock) -> None:
    """Adds the given dataset's information to the datasets dataframe."""
    record: Dict[str, Any] = {
        'id': dataset['id'],
        'title_en': dataset['title_translated']['en'],
        'title_fr': dataset['title_translated']['fr'],
        'date_published': dataset['date_published'],
        'metadata_created': dataset['metadata_created'],
        'metadata_modified': dataset['metadata_modified'],
        'num_resources': dataset['num_resources'],
        'maintainer_email': dataset['maintainer_email'],
        'collection': dataset['collection'],
        'frequency': dataset['frequency'],
        'needs_update': None # TO BE IMPLEMENTED (using frequency)
    }
    lock.acquire()
    datasets.loc[len(datasets)] = record # type: ignore
    lock.release()

def add_resource_ID(dataset: dict, resources_IDs: List[str], 
                    resources_IDs_lock: threading.Lock) -> None:
    """Adds all of the given dataset's resources IDs to the given general 
    list of resources IDs.
    """
    resources_IDs_sublist: List[str] = [
        res['id'] for res in dataset['resources']
    ]
    resources_IDs_lock.acquire()
    resources_IDs.extend(resources_IDs_sublist)
    resources_IDs_lock.release()

def add_resource(resource: dict, resources: pd.DataFrame, 
                 lock: threading.Lock) -> None:
    """Inserts the given dataset's information in the resources dataframe."""

    try:
        # head requests the headers of the url (including the status code), 
        # without loading the whole page
        url_status: int = requests.head(resource['url']).status_code
        
        record: Dict[str, Any] = {
            'id': resource['id'],
            'title_en': resource['name'],
            'title_fr': None, # special field (key changes depending on resource)
            'created': resource['created'],
            'metadata_modified': None, # non-mandatory field
            'format': resource['format'],
            'en': "en" in resource['language'],
            'fr': "fr" in resource['language'],
            'package_id': resource['package_id'],
            'resource_type': resource['resource_type'],
            'url': resource['url'],
            'url_status': url_status
        }
        # non-mandatory and special fieldsL
        if 'metadata_modified' in resource.keys():
            record['metadata_modified'] = resource['metadata_modified']   
        if 'fr' in resource['name_translated'].keys():
            record['title_fr'] = resource['name_translated']['fr']   
        elif 'fr-t-en' in resource['name_translated'].keys():
            record['title_fr'] = resource['name_translated']['fr-t-en'] 

        lock.acquire()
        resources.loc[len(resources)] = record # type: ignore
        lock.release()
    except Exception as e:
        print(type(e))
        print(e.args)
        print(e)



def collect_dataset(catalogue: DataCatalogue, id: str,
                     datasets: pd.DataFrame, datasets_lock: threading.Lock,
                     resources_IDs: List[str], 
                     resources_IDs_lock: threading.Lock,
                     pbar: Optional[tqdm] = None) -> None:
    """Fetches the dataset's relevant information from the online given catalogue,
    adds it to the given datasets inventory, and adds the lists of its resources IDs
    to the given resources IDs list
    """
    dataset: dict = catalogue.get_dataset(id)
    # adds dataset to the common dataframe
    add_dataset(dataset, datasets, datasets_lock)
    # adds its resources IDs to the list of resources IDs
    add_resource_ID(dataset, resources_IDs, resources_IDs_lock)
    if isinstance(pbar, tqdm): # false if pbar == None
        pbar.update()

def collect_resource(catalogue: DataCatalogue, id: str,
                      resources: pd.DataFrame, 
                      lock: threading.Lock, 
                      pbar: Optional[tqdm] = None) -> None:
    """Fetches the resource's relevant information from the online given catalogue
    and adds it to the given resources inventory.
    """
    resource: dict = catalogue.get_resource(id)
    # adds resource to the common dataframe
    add_resource(resource, resources, lock)
    if isinstance(pbar, tqdm): # false if pbar == None
        pbar.update()


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

    datasets.sort_values(by='date_published', ascending=False, inplace=True)
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

    resources.sort_values(by='package_id', inplace=True)
    resources.reset_index(drop=True, inplace=True)
    print(f'-- All {len(resources)} resources\' information was collected.  ({end-start:.2f}s)')

    # Exporting inventories
    
    print()
    timestamp: str = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename1_xlsx: str = f'./../inventories/datasets_inventory_{timestamp}.xlsx'
    datasets.to_excel(filename1_xlsx, index=False)
    print(f'Datasets inventory was successfully exported to {filename1_xlsx}.')
    filename1_json: str = f'./../inventories/datasets_inventory_{timestamp}.json'
    datasets.to_json(filename1_json, index=False)
    print(f'Datasets inventory was successfully exported to {filename1_json}.')

    filename2_xlsx: str = f'./../inventories/resources_inventory_{timestamp}.xlsx'
    resources.to_excel(filename2_xlsx, index=False)
    print(f'Resources inventory was successfully exported to {filename2_xlsx}.')
    filename2_json: str = f'./../inventories/resources_inventory_{timestamp}.json'
    resources.to_json(filename2_json, index=False)
    print(f'Resources inventory was successfully exported to {filename2_json}.')


if __name__ == '__main__':
    main()