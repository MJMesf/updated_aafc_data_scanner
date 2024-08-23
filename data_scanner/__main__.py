"""Parses AAFC's open data, both on the departmental AAFC Open Data Catalogue 
and on Canada's Open Government Portal, to provide the user with a complete 
inventory of datasets and resources, along with visualizations and statistics 
about all the published data.

NOTE: Problem accessing AAFC Open Data Catalogue, task left for later; 
current focus is on the Open Government Portal.
"""

import atexit
import calendar
import datetime as dt
import pandas as pd
from typing import Dict, List
import urllib3
import warnings

from .constants import *
from .tools import TenaciousSession, DataCatalogue, Inventory



@atexit.register
def display_exit_message() -> None:
    """Displays a message when program ends."""
    print("\n---- Program ended.\n")


# HELPER FUNCTIONS ***********************************************************

def date_ago(n: float, unit: str, 
             from_: dt.datetime = dt.datetime.now()) -> dt.datetime:
    """Returns the date n units ago (unit can be day/week/month/year), 
    starting from the given from_ date if provided, from the current date 
    otherwise.
    """
    if n < 0:
        raise ValueError(f"Illegal argument (n = {n}). n must be >= 0")
    date: dt.datetime
    match unit:
        case 'day':
            duration = dt.timedelta(days=n)
            date = from_ - duration
        case 'week':
            duration = dt.timedelta(weeks=n)
            date = from_ - duration
        # no dt.timedelta native construct for months and years
        case 'month':
            # if n is integer
            if n % 1 == 0:
                date = from_
                # search year
                n_from_jan: int = int(n) - date.month + 1
                if n_from_jan > 0:
                    n_years: int = (n_from_jan - 1) // 12 + 1
                    date = date.replace(year=date.year-n_years)
                # search month
                month: int = int(from_.month - n) % 12
                if month == 0: 
                    month = 12  # 12 modulo 12 = 0 -> December
                # check if day is not out of range for that month
                day_max: int
                _, day_max = calendar.monthrange(date.year, month)
                if from_.day > day_max: 
                    date.replace(day=day_max)
                date = date.replace(month=month)
            # if n is decimal
            else: 
                decimals: float = n % 1
                decimals_in_days: int = round(30.43 * decimals)
                days_delta = dt.timedelta(days=decimals_in_days)
                base_date: dt.datetime = date_ago(int(n), 'month', from_)
                date = base_date - days_delta
        case 'year':
            n_in_months: int = round(n * 12) # e.g. 0.33 year -> 4 months
            date: dt.datetime = date_ago(n_in_months, 'month', from_)
        case _:
            raise ValueError(f'Illegal argument (unit = {unit}). Allowed' +\
                             ' values are day, week, month and year.')
    return date


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

def get_up_to_date(ds: pd.Series,
                 now: dt.datetime = dt.datetime.now()) -> bool:
    """Computes currency based on the frequency and last date modified of the 
    dataset. Returns True if dataset is up to date and False if it needs 
    update or cannot read the frequency. (Note: readable frequencies are 
    stored in formats as P1D, P3W, P6M, P1Y, etc.)
    """
    
    # Computing oldest date considered as valid to be up to date
    frequency: str = ds['frequency']
    if frequency.startswith('P') and frequency != 'PT1S':
        full_unit: Dict[str, str] = {'D': 'day', 'W': 'week', 
                                    'M': 'month', 'Y': 'year'}
        unit: str = full_unit[frequency[-1]]
        n: float = float(frequency[1:-1]) # e.g. 1, 2, 0.5 ...
        oldest_valid_update: dt.datetime = date_ago(n, unit, from_=now)
    else:
        # no update explicitly planned
        return True

    # Getting last modified date, based on resources
    last_modified = dt.datetime.fromisoformat(ds['modified'])
    if last_modified >= oldest_valid_update:
        return True
    else:
        return False
    
def get_official_lang(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
    """Returns True if dataset ds is compliant with official languages 
    requirements; false otherwise (i.e. checks whether there are as many 
    resources in English as there are in French).
    """
    resources = all_resources[all_resources['dataset_id'] == ds['id']]
    num_eng: int = 0
    num_fra: int = 0
    for _, res in resources.iterrows():
        if 'eng' in res['lang']:
            num_eng += 1
        if 'fra' in res['lang']:
            num_fra += 1
    return num_eng == num_fra

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

def get_spec(ds: pd.Series, all_resources: pd.DataFrame) -> bool:
    """Returns True if dataset ds is compliant with specification / data 
    dictionary requirements; false otherwise (i.e. if the dataset has 1+ 
    resource classed as 'dataset', checks if it also has resources whose title 
    include "data dictionary" or "specification"; if not, is non-compliant).
    """
    warnings.filterwarnings('ignore', category=UserWarning)
    resources = all_resources[all_resources['dataset_id'] == ds['id']].copy()
    if 'dataset' in list(resources['resource_type']):
        if resources['title_en'].str.contains(
                r'(data dictionary|specification|^dd[_\-]|[_\-]dd.)', 
                case=False).sum():
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
    inventory.datasets['up_to_date'] = inventory.datasets.apply(get_up_to_date, axis=1)
    # Check official languages compliancy
    print('Verifying official languages compliance.')
    inventory.datasets['official_lang'] = inventory.datasets.apply(lambda ds: get_official_lang(ds, inventory.resources), axis=1)
    # Check open formats compliancy
    print('Verifying open formats compliance.')
    inventory.datasets['open_formats'] = inventory.datasets.apply(lambda ds: get_open_formats(ds, inventory.resources), axis=1)
    # Check specification
    print('Verifying specification / data dictionary compliance.')
    inventory.datasets['spec'] = inventory.datasets.apply(lambda ds: get_spec(ds, inventory.resources), axis=1)

    print("Inventories are ready.")

    # Exporting inventories
    print()
    inventory.export_datasets(path='./../inventories/')
    inventory.export_resources(path='./../inventories/')


if __name__ == '__main__':
    main()