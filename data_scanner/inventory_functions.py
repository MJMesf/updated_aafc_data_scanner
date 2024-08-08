"""Functions for collecting data from given catalogues and IDs, 
and adding datasets and resources to the given inventories.
"""

import datetime as dt
import pandas as pd
import threading

from tqdm import tqdm
from typing import List, Dict, Any, Optional

from .__main__ import REGISTRY_DATASETS_BASE_URL, CATALOGUE_DATASETS_BASE_URL, \
    REGISTRY_RESOURCES_BASE_URL, CATALOGUE_RESOURCES_BASE_URL
from .__main__ import DataCatalogue
from .__main__ import get_head_and_retry, infer_name_from_email


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