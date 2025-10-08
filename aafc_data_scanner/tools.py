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
import sys
import subprocess
from requests.adapters import HTTPAdapter, Retry
from selenium.webdriver import Edge
from selenium.webdriver import EdgeOptions
from selenium.webdriver.edge.service import Service
from pathlib import Path
from shutil import which

#imports to keep WebDriver up to date
from selenium.common.exceptions import SessionNotCreatedException, WebDriverException
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService



def _runtime_dir() -> Path:
    if getattr(sys, 'frozen', False):
        # PyInstaller
        if hasattr(sys, '_MEIPASS'):         # one-file temp dir
            return Path(sys._MEIPASS)
        return Path(sys.executable).parent   # one-folder
    return Path(__file__).resolve().parent   # dev: package folder


def _edge_version(edge_path: str = None) -> str | None:
    """
    Try multiple strategies to get Edge version:
      1) BLBeacon registry keys (HKCU/HKLM, 32/64)
      2) edge executable --version (typical)
      3) Common install paths (Program Files, per-user AppData)
    Returns full string like '141.0.3537.57' or None.
    """
    # 1) Registry (Windows)
    try:
        import winreg
        reg_paths = [
            (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Microsoft\Edge\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Edge\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\Edge\BLBeacon"),
        ]
        for hive, subkey in reg_paths:
            try:
                with winreg.OpenKey(hive, subkey) as k:
                    for name in ("version", "pv"):
                        try:
                            val, _ = winreg.QueryValueEx(k, name)
                            if isinstance(val, str) and re.search(r"\d+\.\d+\.\d+\.\d+", val):
                                return val
                        except FileNotFoundError:
                            pass
            except FileNotFoundError:
                continue
    except Exception:
        pass

    # 2) edge --version (if we can run it)
    candidates = []
    if edge_path:
        candidates.append(edge_path)
    candidates += [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        str((Path.home() / r"AppData\Local\Microsoft\Edge\Application\msedge.exe")),
    ]
    for exe in [c for c in candidates if c]:
        try:
            out = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=2)
            m = re.search(r"(\d+\.\d+\.\d+\.\d+)", out.stdout)
            if m:
                return m.group(1)
        except Exception:
            continue

    return None

def _driver_version(driver_path: str) -> str | None:
    """
    Returns full EdgeDriver version string, e.g., '141.0.3537.57', or None.
    """
    try:
        out = subprocess.run([driver_path, "--version"], capture_output=True, text=True, timeout=2)
        # 'MSEdgeDriver 141.0.3537.57 (....)'
        m = re.search(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", out.stdout)
        return m.group(0) if m else None
    except Exception:
        return None

def _major(ver: str | None) -> str | None:
    return ver.split(".", 1)[0] if ver else None

def _find_cached_edgedriver_for_major(major: str) -> str | None:
    """
    Try to locate a matching EdgeDriver in common cache locations
    (webdriver_manager default cache).
    """
    # ~/.wdm/drivers/edgedriver/win64/<version>/msedgedriver.exe
    base = Path.home() / ".wdm" / "drivers" / "edgedriver"
    if not base.exists():
        return None
    # search both win32 and win64 folders
    for arch in ("win64", "win32"):
        root = base / arch
        if not root.exists():
            continue
        # versions are subdirectories; pick newest that matches major
        for version_dir in sorted(root.glob("*"), key=lambda p: p.name, reverse=True):
            if version_dir.is_dir() and version_dir.name.startswith(f"{major}."):
                candidate = version_dir / "msedgedriver.exe"
                if candidate.exists():
                    return str(candidate)
    return None

def _resolve_edgedriver_path_unchecked() -> str:
    """
    Offline resolver without version checks:
      Search order:
        1) <runtime>/drivers/msedgedriver.exe
        2) <runtime>/../drivers/msedgedriver.exe  (repo root in dev)
        3) EDGE_DRIVER_PATH (if set)
        4) msedgedriver found on PATH
    Raises with clear instructions if nothing is found.
    """
    # place next to the app
    p1 = _runtime_dir() / "drivers" / "msedgedriver.exe"
    if p1.exists():
        return str(p1)

    p2 = _runtime_dir().parent / "drivers" / "msedgedriver.exe"
    if p2.exists():
        return str(p2)

    env_path = os.getenv("EDGE_DRIVER_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    exe = which("msedgedriver")
    if exe:
        return exe

    raise RuntimeError(
        "No EdgeDriver found.\n"
        "Put the driver at one of these locations:\n"
        "  <app>/drivers/msedgedriver.exe   (next to the EXE when packaged)\n"
        "  <repo-root>/drivers/msedgedriver.exe \n"
        "Or set EDGE_DRIVER_PATH to the driver file.\n"
        "Then run again."
    )


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
        retries = Retry(
            total=2, connect=2, read=2, status=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["HEAD", "GET"]),
            raise_on_status=False
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        if self.skip_ssl:
            self.session.verify = False
        self.session.headers.update({"User-Agent": "AAFC-Scanner/1.0 (+requests)"})

    def get_and_retry(self, url: str) -> requests.Response:
        return self.session.get(url, allow_redirects=True, timeout=(5, 10))

    def head_and_retry(self, url: str) -> requests.Response:
        return self.session.head(url, allow_redirects=True, timeout=(5, 10))

    def get_status_code(self, url: str) -> int:
        try:
            status_code: int = self.head_and_retry(url).status_code
        except Exception:
            return -1  # fast-fail on network/SSL/connect errors

        if status_code != 404 and re.search(r'atlas/rest|atlas/services', url):
            status_code = 300
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

        # ----- Profile isolation (keep your behavior) -----
        profile_dir = tempfile.mkdtemp(prefix="EdgeProfileUnique_")
        options.add_argument(f"--user-data-dir={profile_dir}")

        # ----- Flags (keep yours; avoid headless if SSO required) -----
        # If you ever need headless anyway, prefer modern flag:
        options.add_argument("--headless=new")
        options.add_argument("disable-gpu")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument("--log-level=3")

        # ----- Resolve driver path without version checks -----
        driver_path = _resolve_edgedriver_path_unchecked()
        service = EdgeService(driver_path)

        # ----- Try to start; on mismatch, show explicit fix -----
        try:
            self.driver = Edge(service=service, options=options)

        except SessionNotCreatedException as e:
            # Most likely: driver/browser version mismatch.
            # Try to detect both versions to help the user.
            drv_ver = _driver_version(driver_path) or "unknown"
            try:
                edge_ver = _edge_version() or "unknown"
            except Exception:
                edge_ver = "unknown"

            raise RuntimeError(
                "Microsoft Edge and EdgeDriver appear to be mismatched.\n"
                f"  • EdgeDriver at: {driver_path}\n"
                f"  • EdgeDriver version: {drv_ver}\n"
                f"  • Microsoft Edge version: {edge_ver}\n\n"
                "Fix: Replace the driver with the SAME-MAJOR version as Edge.\n"
                "Place it at:\n"
                "  <app>/drivers/msedgedriver.exe  (when packaged)\n"
                "  <repo-root>/drivers/msedgedriver.exe  (during development)\n"
                "Or set EDGE_DRIVER_PATH to the correct file.\n"
            ) from e

        except WebDriverException as e:
            # Other startup issues (e.g., permissions)
            raise RuntimeError(f"Failed to start Edge WebDriver: {e}")

        # ----- Optional: log versions after successful start -----
        try:
            caps = self.driver.capabilities
            bver = caps.get("browserVersion") or caps.get("version")
            info = caps.get("msedge", {}) or caps.get("chrome", {}) or {}
            dver = info.get("chromedriverVersion", "")
            print(f"[Edge OK] Browser {bver} | Driver {dver}")
        except Exception:
            pass



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
