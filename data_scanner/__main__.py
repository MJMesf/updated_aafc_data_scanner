"""Parses AAFC's open data, both on the departmental AAFC Open Data Catalogue 
and on Canada's Open Government Portal, to provide the user with a complete 
inventory of datasets and resources, along with visualizations and statistics 
about all the published data.

NOTE: Problem accessing AAFC Open Data Catalogue, task left for later; 
current focus is on the Open Government Portal.
"""

import atexit
import datetime as dt
import pandas as pd
import re
from typing import List
import urllib3
import warnings

from .constants import *
from .tools import TenaciousSession, DataCatalogue, Inventory



@atexit.register
def display_exit_message() -> None:
    """Displays a message when program ends."""
    print("\n---- Program ended.\n")


# HELPER FUNCTIONS ***********************************************************
        

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
    formats = pd.read_csv('./helper_tables/formats.csv')

    resources = resources.merge(formats, how='left', on='format')
    for elem in resources.groupby('format_type')['open'].unique():
        if True not in elem:
            # if a format type doesn't have a single resource in an open format
            return False

    return True

def get_spec_compliance(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
    """Returns True if dataset ds is compliant with specification / data 
    dictionary requirements; false otherwise (i.e. if the dataset has 1+ 
    resource classed as 'dataset', checks if it also has resources whose title 
    include "data dictionary" or "specification"; if not, is non-compliant).
    """
    warnings.filterwarnings('ignore', category=UserWarning)
    resources = all_resources[all_resources['dataset_id'] == ds['id']].copy()
    if 'dataset' in list(resources['resource_type']):
        if resources['title_en'].str.contains(
                r'(data dictionary|specification)', case=False).sum():
            return True
        return False
    # no dataset => no need for data dictionary/specification
    return True



# MAIN CODE ******************************************************************


def main() -> None:

    # Disable warnings (as non-verified SSL Certificates for the catalogue)
    urllib3.disable_warnings()

    # Prepare request session with a set Retry to handle Connection errors
    session = TenaciousSession()

    registry = DataCatalogue(REGISTRY_BASE_URL, session)
    inventory = Inventory()
    inventory.inventory(registry)

    # Computes missing fields of datasets inventories

    # Compute modified for datasets
    print('Completing datasets\' modified dates.')
    inventory.datasets['modified'] = inventory.datasets.apply(lambda ds: get_modified(ds, inventory.resources), axis=1)
    # Check currency for datasets
    print('Verifying datasets\' currency.')
    inventory.datasets['currency'] = inventory.datasets.apply(get_currency, axis=1)
    # Check official_langs
    print('Verifying official languages compliance.')
    inventory.datasets['official_langs'] = inventory.datasets.apply(lambda ds: get_official_langs(ds, inventory.resources), axis=1)
    # Check open_formats
    print('Verifying open formats compliance.')
    inventory.datasets['open_formats'] = inventory.datasets.apply(lambda ds: get_open_formats(ds, inventory.resources), axis=1)
    # Check spec_compliance
    print('Verifying specification / data dictionary compliance.')
    inventory.datasets['spec_compliance'] = inventory.datasets.apply(lambda ds: get_spec_compliance(ds, inventory.resources), axis=1)

    print("Inventories are ready.")

    # Exporting inventories
    print()
    inventory.export_datasets(path='./../inventories/')
    inventory.export_resources(path='./../inventories/')


if __name__ == '__main__':
    main()