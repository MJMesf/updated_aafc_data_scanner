"""Parses AAFC's open data on Canada's Open Government Portal (as well as the 
departmental AAFC Open Data Catalogue in a further version), to provide the 
user with a complete inventory of datasets and resources in csv files.
"""

import atexit
from typing import List, NoReturn
import warnings
from colorama import Fore

from .constants import REGISTRY_BASE_URL, CATALOGUE_BASE_URL, AAFC_ORG_ID
from .tools import RequestsDataCatalogue, DriverDataCatalogue
from .inventories import Inventory




warnings.filterwarnings('ignore', category=FutureWarning)


@atexit.register
def display_exit_message() -> NoReturn:
    """Closes WebDriver(s) and asks user to click enter when program ends, so 
    user has time to read all logged messages if needed before closing 
    console.
    """
    for var in globals():
        if isinstance(var, DriverDataCatalogue):
            var.driver.close()
    print(Fore.CYAN + '\nClick Enter to exit.' + Fore.RESET)
    input()


def main() -> NoReturn:
    """Main code."""

    print()
    print(Fore.YELLOW + '\tAAFC Data Scanner' + Fore.RESET)

    # prompts user for catalogue's check
    must_scan_catalogue = False
    print('\nReady to scan information from open.canada.ca registry.')
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

    # PHASE 1: Inventorying the whole registry

    registry = RequestsDataCatalogue(REGISTRY_BASE_URL)
    registry_datasets: List[str] = registry.search_datasets(
        owner_org=AAFC_ORG_ID)
    print(Fore.GREEN)
    print(f'{len(registry_datasets)} datasets were found on the registry.' + Fore.RESET)
    inventory.inventory(registry, registry_datasets)
    print(Fore.MAGENTA)
    print("\nCompleted Scan of Registry\n")
    print(Fore.RESET)

    if must_scan_catalogue:

        # PHASE 2: Adding datasets from the catalogue
        print("\nScanning Data Catalogue\n")
        # Listing datasets on catalogue
        catalogue = DriverDataCatalogue(CATALOGUE_BASE_URL)
        catalogue_datasets: List[str] = catalogue.list_datasets()
        to_parse: List[str] = [id for id in catalogue_datasets
                               if id not in registry_datasets]
        already_parsed : List[str] = [id for id in catalogue_datasets
                                      if id in registry_datasets]
        if len(to_parse) == 0:
            print(Fore.GREEN)
            print('No additional datasets were found on AAFC Open Data',
                  'Catalogue.' + Fore.RESET)
        else:
            print(Fore.GREEN)
            print(f'{len(to_parse)} additional datasets',
                'were found on AAFC Open Data Catalogue.' + Fore.RESET)

        # For those already on registry, update inventories
        if already_parsed:
            print('\nUpdating catalogue info of already-parsed datasets...')
            inventory.update_platform_info('catalogue', catalogue,
                                           already_parsed)

        # Adding datasets that are only on the departmental catalogue
        if to_parse:
            print('\nNow extracting info of catalogue datasets that were not '
                  'parsed yet.')
            inventory.inventory(catalogue, to_parse)

            # PHASE 3: Verifying catalogue's datasets availability on registry

            print('\nChecking if some of the catalogue\'s datasets are on',
                  'the open registry too, \npublished by another department',
                  'in partnership with AAFC.')
            inventory.update_platform_info('registry', registry, to_parse)


    # FINISHING

    # Announcing total number of datasets and resources
    print()
    print(Fore.YELLOW + f'{len(inventory.datasets)}' + Fore.RESET,
            'datasets and',
            Fore.YELLOW + f'{len(inventory.resources)}' + Fore.RESET,
            'resources were found.')

    # Adding modified dates and compliances checks
    inventory.complete_missing_fields()
    # Completing empty fields
    inventory.datasets = inventory.datasets.fillna({
        'on_registry': False,
        'on_catalogue': False,
        'org': 'aafc-aac',
        'org_title': 'Agriculture and Agri-Food Canada'})

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
