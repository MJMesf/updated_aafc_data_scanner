"""Parses AAFC's open data, both on the departmental AAFC Open Data Catalogue 
and on Canada's Open Government Portal, to provide the user with a complete 
inventory of datasets and resources, along with visualizations and statistics 
about all the published data.

NOTE: Problem accessing AAFC Open Data Catalogue, task left for later; 
current focus is on the Open Government Portal.
"""

import urllib3

from .constants import *
from .tools import TenaciousSession, DataCatalogue
from .inventories import Inventory


def main() -> None:

    # Disabling warnings (as non-verified SSL Certificates for the catalogue)
    urllib3.disable_warnings()

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
    
    print()
    print('Click Enter to exit.')
    input()


if __name__ == '__main__':
    main()