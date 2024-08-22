"""Classes used by the main program to handle scanning and storing of the 
datasets information.
"""

from colorama import Fore, Style, init
import concurrent.futures
from dataclasses import dataclass, field
import datetime as dt
import pandas as pd
import re
import requests
from requests.adapters import HTTPAdapter, Retry
import threading
import time
from tqdm import tqdm
from typing import Any, Dict, List, Optional

from .constants import *


@dataclass
class TenaciousSession:
    """A requests Session set at construct time to retry any request attempt 
    due to Connection errors (Url statuses 500, 502, 503, 504).
    """

    session: requests.Session

    def __init__(self):
        self.session = requests.Session()
        retries = Retry(backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def get_and_retry(self, url):
        """Sends http request and retries in case of connection issues."""
        return self.session.get(url)
    
    def get_head_and_retry(self, url):
        """Sends http request and retries in case of connection issues."""
        return self.session.head(url)
    

@dataclass
class DataCatalogue:
    """A class representing a CKAN data catalogue, as Canada's Open Data registry, and ."""
    
    base_url: str
    """Base url of catalogue, to which API commands are appended"""

    session: TenaciousSession
    """TenaciousSession session used to make API requests and others"""

    def __init__(self, base_url: str, 
                 session: TenaciousSession = TenaciousSession()) -> None:
        self.base_url = base_url
        self.session = session

    def request_ckan(self, url: str) -> Any:
        """Sends a CKAN API web request with a given URL and return the content 
        of the result
        """
        response: requests.models.Response
        # Pronlem accessing AAFC Open Data Catalogue; TO BE FIXED LATER
        if url.startswith('https://data-catalogue-donnees.agr.gc.ca/'):
            # DataCatalogue has issues with SSL Certification
            response = requests.get(url, verify=False)
        else:
            response = self.session.get_and_retry(url)
        assert response.status_code == 200, \
            f'Request Error:\nUnexpected status code: {response.status_code}'
        data = response.json()
        assert data['success'] == True, \
            f'CKAN API Error: request\'s success is False'
        return data['result']

    def list_datasets(self) -> List[str]:
        """Returns list of all datasets (packages) IDs in the catalogue"""
        url: str = self.base_url + 'package_list'
        return self.request_ckan(url)
    
    def search_datasets(self, **kwargs: str) -> List[str]:
        """Returns IDs of datasets that match the given filters
        e.g. groups='test-group'
        """
        filters: str = '+'.join(f'{key}:{val}' for key, val in kwargs.items())
        url: str = self.base_url + 'package_search?fq=' + filters
        # checks total number of results
        count: int = self.request_ckan(url)['count']
        
        # creates a list to be filled with datasets' IDs
        list_id: List[str] = []
        i: int = 0
        sublist_id: List[str]
        results: List[dict]

        # get all IDs 100 by 100
        while len(list_id) < count:
            filters = '+'.join(f'{key}:{val}' for key, val in kwargs.items())
            url = self.base_url + f'package_search?rows=100&start={i}&fq=' + filters
            results = self.request_ckan(url)['results']
            sublist_id = [dataset['id'] for dataset in results]
            list_id.extend(sublist_id)
            i += 100
        return list_id
    
    def get_dataset(self, id: str) -> dict:
        """Returns dataset's information, given its ID"""
        url: str = self.base_url + f'package_show?id={id}'
        return self.request_ckan(url)
    
    def get_resource(self, id: str) -> dict:
        """Returns resource's information, given its ID"""
        url: str = self.base_url + f'resource_show?id={id}'
        return self.request_ckan(url)
    
@dataclass
class Inventory:

    datasets: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=DATASETS_COLS)
    )

    resources: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=RESOURCES_COLS)
    )

    @staticmethod
    def infer_name_from_email(email: str) -> str:
        """(Utility method) Infer name of the email owner from the given email
        address (splits and capitalizes words before @).
        """
        
        def upper_after_mac(m: re.Match) -> str:
            return m.group(1) + m.group(2).upper()
        
        name: str = ' '.join(re.split(r'[.\-_]', 
                                    email.split('@')[0].lower())).title()
        name = re.sub(r'(Ma?c)([a-z])', upper_after_mac, name)
        name = re.sub(r'^MacKenzie', 'Mackenzie', name)
        return name

    def add_dataset(self, dataset: dict, datasets: pd.DataFrame,
                lock: threading.Lock) -> None:
        """Adds the given dataset's information to the datasets dataframe.
        The lock argument is a mutex on the datasets dataframe."""

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
        record['maintainer_name'] = Inventory.infer_name_from_email(
            record['maintainer_email'])
        record['collection'] = dataset['collection']
        record['frequency'] = dataset['frequency']
        record['registry_link'] = REGISTRY_DATASETS_BASE_URL.format(record['id'])
        record['catalogue_link'] = CATALOGUE_DATASETS_BASE_URL.format(record['id'])
        # 'modified', 'currency', 'official_langs' and 'spec_compliance' 
        # will be added to the record later on

        lock.acquire()
        datasets.loc[len(datasets)] = record # type: ignore
        lock.release()

    def add_resource(self, resource: dict, resources: pd.DataFrame, 
                    lock: threading.Lock) -> None:
        """Inserts the given resource's information in the resources dataframe.
        The lock argument is a mutex on the given resources dataframe."""

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
            record['url_status'] = TenaciousSession().get_head_and_retry(
                resource['url']).status_code
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

    def _collect_dataset_with_resources(
            self, dc: DataCatalogue, id: str,
            datasets_lock: threading.Lock,
            resources_lock: threading.Lock,
            pbar: Optional[tqdm] = None) -> None:
        """Fetches the information of the id'd dataset from the given 
        DataCatalogue dc, along with its resources information, and stores it 
        in self datasets and resources dataframes, both of which need a 
        provided mutex/lock in the arguments.
        """
        dataset: dict = dc.get_dataset(id)
        # adds dataset to the common dataframe
        self.add_dataset(dataset, self.datasets, datasets_lock)
        for resource in dataset['resources']:
            # adds resource to the common dataframe
            self.add_resource(resource, self.resources, resources_lock)
        if isinstance(pbar, tqdm): # false if pbar == None
            pbar.update()

    def inventory(self, dc: DataCatalogue) -> None:
        """Fetches information of all datasets and resources of the given 
        DataCatalogue dc and stores it in self datasets and resources 
        dataframes.
        """

        print()
        print('Collecting information of all datasets ...')
        start = time.time() # times datasets collection

        # listing all the datasets IDs:
        datasets_IDs = dc.search_datasets(owner_org=AAFC_ORG_ID)
        # initializing the progress bar
        pbar = tqdm(desc='Processed Datasets', total=len(datasets_IDs), 
                    colour='green', ncols=100, ascii=' -=') 

        # in parallel threads, collects relevant information of 
        # each dataset and associated resources
        datasets_lock = threading.Lock()
        resources_IDs_lock = threading.Lock()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for id in datasets_IDs:
                executor.submit(self._collect_dataset_with_resources, dc, id, 
                                datasets_lock, resources_IDs_lock, pbar)
        pbar.close()
        end = time.time() # ends datasets collection timer

        self.datasets.sort_values(by='published', ascending=False, inplace=True)
        self.datasets.reset_index(drop=True, inplace=True)
        self.resources.sort_values(by='dataset_id', inplace=True)
        self.resources.reset_index(drop=True, inplace=True)
        print(f'All information was collected.  ({end-start:.2f}s)')
        init()
        print(Fore.YELLOW + f'{len(self.datasets)}' + Fore.RESET,
              'datasets and',
              Fore.YELLOW + f'{len(self.resources)}' + Fore.RESET,
              'resources were found.')
        print()

    def export_datasets(self, path: str = './') -> None:
        """Exports self datasets dataframe as a csv file at the given path, if
        any; if none given, exports it in the current folder.
        """
        self._export_to_csv(self.datasets, 'datasets', path)

    def export_resources(self, path: str = './') -> None:
        """Exports self resources dataframe as a csv file at the given path, if
        any; if none given, exports it in the current folder.
        """
        self._export_to_csv(self.resources, 'resources', path)

    def _export_to_csv(self, df: pd.DataFrame, df_name: str, 
                       path: str) -> None:
        """Exports DataFrame df as a csv file to the given path, if any. 
        Needs also the name of df as a string for outputs.
        """

        if path != './':
                path = re.sub(r'[\\]+', '/', path)
                if not path.endswith('/'):
                    path = path + '/'
        timestamp: str = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename: str = path + f'{df_name}_inventory_{timestamp}.csv'
        msg: str
        init()
        try:
            df.to_csv(filename, index=False, encoding='utf_8_sig')
        except Exception as e:
            msg = f'Error exporting {df_name} inventory to {filename}:\n{e}\n'
            print(Fore.RED + msg + Fore.RESET)
        else:
            msg = f'{
                df_name.capitalize()
            } inventory was successfully exported to {filename}.'
            print(Fore.GREEN + msg + Fore.RESET)

            