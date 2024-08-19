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
from typing import Optional, Dict, List, Any

from .constants import *


# Request Session Preparation (to avoid connection errors 5XX)
session = requests.Session()
retries = Retry(backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))

# Disable warnings (as non-verified SSL Certificates for the catalogue)
urllib3.disable_warnings()

# Prepare some helper tables imports for later user
formats = pd.read_csv('./helper_tables/formats.csv')


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



# REQUESTS FUNCTIONS *********************************************************


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


# INVENTORYING FUNCTIONS *****************************************************


def add_dataset(dataset: dict, datasets: pd.DataFrame,
                lock: threading.Lock) -> None:
    """Adds the given dataset's information to the datasets dataframe."""
    
    record: Dict[str, Any] = {}
    record['id'] = dataset['id']
    record['title_en'] = dataset['title_translated']['en']
    record['title_fr'] = dataset['title_translated']['fr']
    record['published'] = dt.datetime.strptime(
        dataset['date_published'],'%Y-%m-%d %H:%M:%S').isoformat()
    record['metadata_created'] = dataset['metadata_created']
    record['metadata_modified'] = dataset['metadata_modified']
    record['num_resources'] = dataset['num_resources']
    record['maintainer_email'] = dataset['maintainer_email'].lower()
    record['maintainer_name'] = infer_name_from_email(
        record['maintainer_email'])
    record['collection'] = dataset['collection']
    record['frequency'] = dataset['frequency']
    record['registry_link'] = REGISTRY_DATASETS_BASE_URL.format(record['id'])
    record['catalogue_link'] = CATALOGUE_DATASETS_BASE_URL.format(record['id'])
    # 'modified', 'currency' and 'official_langs' 
    # will be added to the record later on

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

        record: Dict[str, Any] = {}
        record['id'] = resource['id']
        record['title_en'] = resource['name']
        record['created'] = resource['created']
        record['format'] = resource['format']
        record['langs'] = '/'.join(resource['language'])
        record['dataset_id'] = resource['package_id']
        record['resource_type'] = resource['resource_type']
        record['url'] = resource['url']
        record['url_status'] = get_head_and_retry(resource['url']).status_code
        record['registry_link'] = REGISTRY_RESOURCES_BASE_URL.format(
            record['dataset_id'], record['id']
        )
        record['catalogue_link'] = CATALOGUE_RESOURCES_BASE_URL.format(
            record['dataset_id'], record['id']
        )
        # non-mandatory and special fields:
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


# HELPER FUNCTIONS ***********************************************************


def infer_name_from_email(email: str) -> str:
    """Infer name of the email owner from the given email address 
    (split and capitalize words before @).
    """
    
    def upper_after_mac(m: re.Match) -> str:
        return m.group(1) + m.group(2).upper()
    
    name: str = ' '.join(re.split(r'[.\-_]', 
                                  email.split('@')[0].lower())).title()
    name = re.sub(r'(Ma?c)([a-z])', upper_after_mac, name)
    name = re.sub(r'^MacKenzie', 'Mackenzie', name)
    return name
        

def get_modified(ds: pd.Series, all_resources: pd.DataFrame) -> str:
    """Returns last date modified of the dataset ds, given the last 
    metadata_modified dates of its resources
    """
    last_modified: dt.datetime
    resources = all_resources[all_resources['dataset_id'] == ds['id']]
    modified_dates: List[dt.datetime] = []
    for _, res in resources.iterrows():
        created = dt.datetime.fromisoformat(res['created'])
        modified: dt.datetime = created
        if pd.notnull(res['metadata_modified']):
            metadata_modified = dt.datetime.fromisoformat(
                res['metadata_modified'])
            modified = max(created, metadata_modified)
        modified_dates.append(modified)
    last_modified = max(modified_dates) # latest date
    return last_modified.isoformat()

def get_currency(ds: pd.Series,
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
    
def get_official_langs(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
    """Returns True if dataset ds is compliant with official languages 
    requirements; false otherwise (i.e. checks whether there are as many 
    resources in English as there are in French).
    """
    resources = all_resources[all_resources['dataset_id'] == ds['id']]
    num_en: int = 0
    num_fr: int = 0
    for _, res in resources.iterrows():
        if 'en' in res['langs']:
            num_en += 1
        if 'fr' in res['langs']:
            num_fr += 1
    return num_en == num_fr

def get_open_formats(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
    """Returns True if dataset ds is compliant with open formats 
    requirements; false otherwise (i.e. checks for each format type, 
    assuming each format type contains the same data, that at least one
    type is open).
    """
    resources = all_resources[all_resources['dataset_id'] == ds['id']].copy()
    global formats

    resources = resources.merge(formats, how='left', on='format')
    for elem in resources.groupby('format_type')['open'].unique():
        if True not in elem:
            # if a format type doesn't have a single resource in an open format
            return False

    return True


# MAIN CODE ******************************************************************


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
    print('Completing datasets\' modified dates.')
    datasets['modified'] = datasets.apply(lambda ds: get_modified(ds, resources), axis=1)
    # Check currency for datasets
    print('Veritying datasets\' currency.')
    datasets['currency'] = datasets.apply(get_currency, axis=1)
    # Check official_langs
    print('Verifying official languages compliancy.')
    datasets['official_langs'] = datasets.apply(lambda ds: get_official_langs(ds, resources), axis=1)
    # Check open_formats
    print('Verifying open formats compliancy.')
    datasets['open_formats'] = datasets.apply(lambda ds: get_open_formats(ds, resources), axis=1)
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