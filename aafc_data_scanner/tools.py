"""Classes used by the main program to handle scanning and storing of the 
datasets information.
"""

from typing import Any, List
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import json
import re
import requests
import os
import tempfile
import time
from requests.adapters import HTTPAdapter, Retry
from selenium.webdriver import Edge
from selenium.webdriver import EdgeOptions
from selenium.webdriver.edge.service import Service
from pathlib import Path
from shutil import which


@dataclass
class TenaciousSession:
    """A requests Session set at construct time to retry any request attempt 
    due to Connection errors (Url statuses 502, 503, 504).
    """

    session: requests.Session = field(default_factory=requests.Session)
    """A requests Session initialized with specific settings."""

    skip_ssl: bool = False
    """If the session must skip SSL verification (needed for AAFC Open Data 
    Catalogue)
    """

    def __post_init__(self) -> None:
        retries = Retry(backoff_factor=1,
                        status_forcelist=[502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        if self.skip_ssl:
            self.session.verify = False

    def get_and_retry(self, url: str) -> requests.Response:
        """Sends http request and retries in case of connection issues."""
        return self.session.get(url)

    def head_and_retry(self, url: str) -> requests.Response:
        """Gets head of http request (url status code and other info) and 
        retries in case of connection issues.
        """
        return self.session.head(url)

    def get_status_code(self, url: str) -> int:
        """Gets url status code of url and corrects if needed (some ArcGis 
        links appear as 400 or 405 while they are accessible).
        """
        status_code: int = self.head_and_retry(url).status_code
        if status_code != 404 and \
                re.search(r'atlas/rest|atlas/services', url):
            status_code = 300
            # because program would give 500 status code for working links on
            # atlas web map services
        return status_code


@dataclass
class DataCatalogue(ABC):
    """An abstract class representing a CKAN data catalogue, as Canada's Open 
    Data registry, or AAFC Open Data Catalogue.
    """

    base_url: str
    """Base url of catalogue, to which API commands are appended"""

    @abstractmethod
    def request_ckan(self, url: str) -> Any:
        """Makes a request to ckan by the mean set in the subclass (e.g. 
        through a get request or via a selenium webdriver).
        """

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
class RequestsDataCatalogue(DataCatalogue):
    """Subclass of DataCatalogue using TenaciousSession to make requests to 
    the CKAN API.
    """

    session: TenaciousSession = field(default_factory=TenaciousSession)
    """TenaciousSession session used to make API requests and others"""

    # overrides DataCatalogue's abstract method
    def request_ckan(self, url: str) -> Any:
        """Sends a CKAN API web request with a given URL and return the content 
        of the result
        """
        response: requests.models.Response = self.session.get_and_retry(url)
        assert response.status_code == 200, \
            f'Request Error:\nUnexpected status code: {response.status_code}'
        data = response.json()
        assert data['success'], \
            'CKAN API Error: request\'s success is False'
        return data['result']


@dataclass
class DriverDataCatalogue(DataCatalogue):
    """Subclass of DataCatalogue using selenium Edge Driver to make requests 
    to the CKAN API."""

    driver: Edge
    """Selenium webdriver initialized with specific settings to access AAFC 
    Open Data Catalogue without authentication issues (uses Edge for 
    automatic AAFC employee microsoft authentication)
    """

    # overrides dataclass default constructor
    def __init__(self, base_url):
        self.base_url = base_url
        options = EdgeOptions()

        #unique directory to avoid multiple Edge instance issue
        profile_dir = tempfile.mkdtemp(prefix="EdgeProfileUnique_")

        options.add_argument("headless")
        options.add_argument("disable-gpu")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument("--log-level=3")

        from shutil import which
        service = None

        driver_path = os.getenv("EDGE_DRIVER_PATH")
        if driver_path and Path(driver_path).exists():
            service = Service(driver_path)

        else:
            from shutil import which
            exe = which("msedgedriver")
            if exe:
                service = Service(exe)
            else:
                # last resort: webdriver_manager (needs internet)
                from webdriver_manager.microsoft import EdgeChromiumDriverManager
                service = Service(EdgeChromiumDriverManager().install())

        #create unique path so Edge has no other instances
        options.add_argument(f"--user-data-dir={profile_dir}")
        self.driver = Edge(service=service,options=options)

    # overrides DataCatalogue's abstract method
    def request_ckan(self, url: str) -> Any:
        self.driver.get(url)
        self.driver.get(url)
        page_source = self.driver.page_source
      
        try:
            data = json.loads(page_source)
            assert data.get('success') is True, "CKAN API Error: 'success' is False"
            return data['result']
        
        except json.JSONDecodeError:
        # fallback for possibly the page_source has extra HTML
            match = re.search(r'(\{.*\})', page_source, re.DOTALL)

            if match:
                data = json.loads(match.group(1))
                assert data.get('success') is True, "CKAN API Error: 'success' is False"
                return data['result']
            
            else:
                raise Exception("No valid JSON found in page source.")
