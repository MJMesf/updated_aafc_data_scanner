"""Parses AAFC's open data, both on the departmental AAFC Open Data Catalogue 
and on Canada's Open Government Portal, to provide the user with a complete 
inventory of datasets and resources, along with visualizations and statistics 
about all the published data.

NOTE: Problem accessing AAFC Open Data Catalogue, task left for later; 
current focus is on the Open Government Portal
"""

import requests
import urllib3

from dataclasses import dataclass
from typing import List, Dict, Any

AAFC_ODC_BASE_URL = 'https://data-catalogue-donnees.agr.gc.ca/api/action/'
"""Base url to send API requests to AAFC Open Data Catalogue"""
REGISTRY_BASE_URL = 'https://open.canada.ca/data/api/action/'
"""Base url to send API requests to open.canada.ca"""

def request(url: str) -> Any:
    """Sends an CKAN API web request with a given URL and return the content 
    of the result
    """

    response: requests.models.Response
    # Pronlem accessing AAFC Open Data Catalogue; TO BE FIXED LATER
    if url.startswith('https://data-catalogue-donnees.agr.gc.ca/'):
        urllib3.disable_warnings()
        response = requests.get(url, verify=False)
    else:
        response = requests.get(url)
    assert response.status_code == 200, \
        f'Request Error:\nUnexpected status code: {response.status_code}'
    data = response.json()
    assert data['success'] == True, \
        f'CKAN API Error: request\'s success is False'
    return data['result']

@dataclass
class DataCatalogue:
    
    base_url: str
    """Base url of catalogue, to which API commands are appended"""

    def list_datasets(self) -> List[str]:
        """Returns list of all datasets (packages) IDs in the catalogue"""
        url: str = self.base_url + 'package_list'
        return request(url)
    
    def get_dataset(self, id: str) -> Dict[str, Any]:
        """Returns dataset's information, given its ID"""
        url: str = self.base_url + f'package_show?id={id}'
        return request(url)


def main():
    
    # Problem accessing AAFC's Open Data Catalogue; TO BE FIXED LATER
    # aafc_odc = DataCatalogue(AAFC_ODC_BASE_URL)
    registry = DataCatalogue(REGISTRY_BASE_URL)




if __name__ == '__main__':
    main()