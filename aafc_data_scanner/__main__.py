"""Parses AAFC's open data on Canada's Open Government Portal (as well as the 
departmental AAFC Open Data Catalogue in a further version), to provide the 
user with a complete inventory of datasets and resources in csv files.
"""

import atexit
from colorama import Fore

from .constants import *
from .tools import TenaciousSession, DataCatalogue
from .inventories import Inventory


@atexit.register
def display_exit_message() -> None:
    """Asks user to click enter when program ends, so user has time to read 
    all logged messages if needed before closing console.
    """
    print('\nClick Enter to exit.')
    input()


def main() -> None:

    print()
    print(Fore.YELLOW + '\tAAFC Data Scanner' + Fore.RESET)

    # Preparing request session with a set Retry to handle Connection errors
    session = TenaciousSession()

    # Inventorying the whole registry
    registry = DataCatalogue(REGISTRY_BASE_URL, session)
    inventory = Inventory()
    inventory.inventory(registry)

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