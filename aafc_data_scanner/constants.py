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

DATASETS_COLS = [
    'id', 'title_en', 'title_fr', 'published', 'modified',
    'metadata_created', 'metadata_modified', 'num_resources', 
    'on_registry', 'on_catalogue', 
    'org', 'org_title', 'aafc_org', 'aafc_org_title', 
    'maintainer_email', 'maintainer_name', 'collection', 
    'frequency', 'harvested', 'internal',
    'up_to_date', 'official_lang', 'open_formats', 'spec', 
    'registry_link', 'catalogue_link', 
    'creator',
    'data_steward_email',
    'elegible_for_release',
    'jurisdiction',
    'license_title',
    'notes_en',
    'notes_fr',
    'odi_reference_number',
    'organization_description',
    'procured_data',
    'procured_data_organization_name',
    'publication',
    'state',
    'subject'
]
DATASETS_DTYPES = {
    'id': 'string', 'title_en': 'string', 'title_fr': 'string', 
    'published': 'string', 'modified': 'string',
    'metadata_created': 'string', 'metadata_modified': 'string', 
    'num_resources': 'Int64', 'on_registry': 'boolean', 
    'on_catalogue': 'boolean', 'org': 'string', 'org_title': 'string', 
    'aafc_org': 'string', 'aafc_org_title': 'string', 
    'maintainer_email': 'string', 'maintainer_name': 'string', 
    'collection': 'string', 'frequency': 'string', 'harvested': 'boolean', 
    'internal': 'boolean', 'up_to_date': 'boolean', 
    'official_lang': 'boolean', 'open_formats': 'boolean', 'spec': 'boolean', 
    'registry_link': 'string', 'catalogue_link': 'string',
    'creator': 'string',
    'data_steward_email': 'string',
    'elegible_for_release': 'string',
    'jurisdiction': 'string',
    'license_title': 'string',
    'notes_en': 'string',
    'notes_fr': 'string',
    'odi_reference_number' : 'string',
    'organization_description' : 'string',
    'procured_data': 'string',
    'procured_data_organization_name':'string',
    'publication': 'string',
    'state':'string',
    'subject': 'string',
}
RESOURCES_COLS = [
    'id', 'title_en', 'title_fr', 'created', 
    'metadata_modified', 'format', 'lang', 
    'dataset_id', 'resource_type', 'url', 'url_status', 'https',
    'registry_link', 'catalogue_link'
]
RESOURCES_DTYPES = {
    'id': 'string', 'title_en': 'string', 'title_fr': 'string', 
    'created': 'string', 'metadata_modified': 'string',
    'format': 'string', 'lang': 'string', 'dataset_id': 'string',
    'resource_type': 'string', 'url': 'string', 'url_status': 'Int64',
    'https': 'string', 'registry_link': 'string', 'catalogue_link': 'string'
}
