"""This module provides project-wide constants."""


REGISTRY_BASE_URL = 'https://open.canada.ca/data/api/3/action/'
"""Base url to send API requests to open.canada.ca"""
REGISTRY_DATASETS_BASE_URL = \
    'https://open.canada.ca/data/en/dataset/{}'
"""Base url to open a dataset on the registry (open.canada.ca), 
to format with its id
"""
REGISTRY_RESOURCES_BASE_URL = \
    'https://open.canada.ca/data/en/dataset/{}/resource/{}'
"""Base url to open a resource on the registry (open.canada.ca), 
to format with its package/datasets id, along with the resource id
"""

CATALOGUE_BASE_URL = 'https://data-catalogue-donnees.agr.gc.ca/api/3/action/'
"""Base url to send API requests to AAFC Open Data Catalogue"""
CATALOGUE_DATASETS_BASE_URL = \
    'https://data-catalogue-donnees.agr.gc.ca/aafc-open-data/{}'
"""Base url to open a dataset on the AAFC Open Data Catalogue, 
to format with its id
"""
CATALOGUE_RESOURCES_BASE_URL = \
    'https://data-catalogue-donnees.agr.gc.ca/aafc-open-data/{}/resource/{}'
"""Base url to open a resource on the AAFC Open Data Catalogue, 
to format with its package/datasets id, along with the resource id
"""


AAFC_ORG_ID = '2ABCCA59-6C57-4886-99E7-85EC6C719218'
"""ID of the organization AAFC on the Open Registry"""

DATASETS_COLS = ['id', 'title_en', 'title_fr', 'published', 'modified',
                 'metadata_created', 'metadata_modified', 'num_resources', 
                 'maintainer_email', 'maintainer_name', 'collection', 
                 'frequency', 'harvested', 'internal',
                 'up_to_date', 'official_lang', 'open_formats', 'spec', 
                 'registry_link', 'catalogue_link']
RESOURCES_COLS = ['id', 'title_en', 'title_fr', 'created', 
                  'metadata_modified', 'format', 'lang', 
                  'dataset_id', 'resource_type', 'url', 'url_status',
                  'registry_link', 'catalogue_link']
