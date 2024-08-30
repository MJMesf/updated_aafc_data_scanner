"""Classes used by the main program to handle scanning and storing of the 
datasets information.
"""

from dataclasses import dataclass, field
import re
import requests
from requests.adapters import HTTPAdapter, Retry
from typing import Any, List

from .constants import *

@dataclass
class TenaciousSession:
    """A requests Session set at construct time to retry any request attempt 
    due to Connection errors (Url statuses 500, 502, 503, 504).
    """

    session: requests.Session = field(default_factory=requests.Session)
    """A requests Session initialized with specific settings."""

    def __post_init__(self) -> None:
        retries = Retry(backoff_factor=1, 
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def get_and_retry(self, url: str) -> requests.Response:
        """Sends http request and retries in case of connection issues."""
        return self.session.get(url)
    
    def get_head_and_retry(self, url: str) -> requests.Response:
        """Gets head of http request (url status code and other info) and 
        retries in case of connection issues.
        """
        return self.session.head(url)
    
    def get_status_code(self, url: str) -> int:
        """Gets url status code of url and corrects if needed (some ArcGis 
        links appear as 400 or 405 while they are accessible).
        """
        status_code: int = self.get_head_and_retry(url).status_code
        if status_code != 404 and \
                re.search(r'atlas/rest|atlas/services', url):
            status_code = 300
        return status_code
    

@dataclass
class DataCatalogue:
    """A class representing a CKAN data catalogue, as Canada's Open Data registry, and ."""
    
    base_url: str
    """Base url of catalogue, to which API commands are appended"""

    session: TenaciousSession = field(default_factory=TenaciousSession)
    """TenaciousSession session used to make API requests and others"""
    

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
