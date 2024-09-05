"""Parses AAFC's open data on Canada's Open Government Portal (as well as the 
departmental AAFC Open Data Catalogue in a further version), to provide the 
user with a complete inventory of datasets and resources in csv files.
"""

import atexit
from colorama import Fore
from typing import List

from .constants import *
from .tools import RequestsDataCatalogue, DriverDataCatalogue
from .inventories import Inventory


@atexit.register
def display_exit_message() -> None:
    """Asks user to click enter when program ends, so user has time to read 
    all logged messages if needed before closing console.
    """
    print(Fore.CYAN + '\nClick Enter to exit.' + Fore.RESET)
    input()


def main() -> None:

    print()
    print(Fore.YELLOW + '\tAAFC Data Scanner' + Fore.RESET)

    # prompts user for catalogue's check
    must_scan_catalogue = False
    print('\nReady to scan information from open.canaca.ca registry.')
    print('Do you also wish to scan AAFC Open Data Catalogue?')
    print(Fore.CYAN + 'Enter y for yes:' + Fore.RESET, end=" ")
    response = str(input())
    if response.lower() == 'y':
        must_scan_catalogue = True
    
    if must_scan_catalogue:
        print('\nFor the catalogue to be scanned, please make sure Edge is',
              'installed on your computer \nand allows you to automatically',
              'authenticate as an AAFC employee.')

    print('\nCommencing scan.')
    inventory = Inventory()
    
    # Inventorying the whole registry
    registry = RequestsDataCatalogue(REGISTRY_BASE_URL)
    registry_datasets: List[str]
    registry_datasets = registry.search_datasets(owner_org=AAFC_ORG_ID)
    print(Fore.GREEN)
    print(f'{len(registry_datasets)} datasets were found on the registry.' +\
           Fore.RESET)
    inventory.inventory(registry, registry_datasets)

    if must_scan_catalogue:
        # Adding datasets that are only on the departmental catalogue
        catalogue = DriverDataCatalogue(CATALOGUE_BASE_URL)
        catalogue_datasets: List[str] = catalogue.list_datasets()
        only_on_catalogue: List[str] = [
            id for id in catalogue_datasets if id not in registry_datasets
        ]
        if len(only_on_catalogue) == 0:
            print(Fore.GREEN)
            print('No additional datasets were found on AAFC Open Data', 
                  'Catalogue.' + Fore.RESET)
        else:
            print(Fore.GREEN)
            print(f'{len(only_on_catalogue)} additional datasets were',
                'found on AAFC Open Data Catalogue.' + Fore.RESET)
            inventory.inventory(catalogue, only_on_catalogue)

    # Announcing total number of datasets and resources
    print()
    print(Fore.YELLOW + f'{len(inventory.datasets)}' + Fore.RESET,
            'datasets and',
            Fore.YELLOW + f'{len(inventory.resources)}' + Fore.RESET,
            'resources were found.')

    # Adding modified dates and compliances checks
    inventory.complete_missing_fields()

    # Correcting non-existent links to registry & catalogue
    only_on_registry: List[str] = [
        id for id in registry_datasets if id not in catalogue_datasets
    ] 
    inventory.datasets.loc[
        inventory.datasets['id'].isin(only_on_registry), 'catalogue_link'
    ] = None
    inventory.datasets.loc[
        inventory.datasets['id'].isin(only_on_catalogue), 'registry_link'
    ] = None
    inventory.resources.loc[
        inventory.resources['dataset_id'].isin(only_on_registry), 
        'catalogue_link'
    ] = None
    inventory.resources.loc[
        inventory.resources['dataset_id'].isin(only_on_catalogue), 
        'registry_link'
    ] = None

    # Exporting inventories
    print()
    inventory.export_datasets(path='./inventories/')
    inventory.export_resources(path='./inventories/')
    inventory.export_datasets(path='./inventories/',
                              filename='_latest_datasets_inventory.csv')
    inventory.export_resources(path='./inventories/',
                               filename='_latest_resources_inventory.csv')


if __name__ == '__main__':
    main()