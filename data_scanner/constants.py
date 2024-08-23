"""This module provides project-wide constants"""


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
                 'frequency', 'up_to_date', 'official_lang', 'open_formats',
                 'spec', 'registry_link', 'catalogue_link']
RESOURCES_COLS = ['id', 'title_en', 'title_fr', 'created', 
                  'metadata_modified', 'format', 'lang', 
                  'dataset_id', 'resource_type', 'url', 'url_status',
                  'registry_link', 'catalogue_link']

ISO639_MAP = {
    'aa': 'aar', 'ab': 'abk', 'af': 'afr', 'ak': 'aka', 'am': 'amh', 
    'ar': 'ara', 'an': 'arg', 'as': 'asm', 'av': 'ava', 'ae': 'ave', 
    'ay': 'aym', 'az': 'aze', 'ba': 'bak', 'bm': 'bam', 'be': 'bel', 
    'bn': 'ben', 'bi': 'bis', 'bo': 'bod', 'bs': 'bos', 'br': 'bre', 
    'bg': 'bul', 'ca': 'cat', 'cs': 'ces', 'ch': 'cha', 'ce': 'che', 
    'cu': 'chu', 'cv': 'chv', 'kw': 'cor', 'co': 'cos', 'cr': 'cre', 
    'cy': 'cym', 'da': 'dan', 'de': 'deu', 'dv': 'div', 'dz': 'dzo', 
    'el': 'ell', 'en': 'eng', 'eo': 'epo', 'et': 'est', 'eu': 'eus', 
    'ee': 'ewe', 'fo': 'fao', 'fa': 'fas', 'fj': 'fij', 'fi': 'fin', 
    'fr': 'fra', 'fy': 'fry', 'ff': 'ful', 'gd': 'gla', 'ga': 'gle', 
    'gl': 'glg', 'gv': 'glv', 'gn': 'grn', 'gu': 'guj', 'ht': 'hat', 
    'ha': 'hau', 'sh': 'hbs', 'he': 'heb', 'hz': 'her', 'hi': 'hin', 
    'ho': 'hmo', 'hr': 'hrv', 'hu': 'hun', 'hy': 'hye', 'ig': 'ibo', 
    'io': 'ido', 'ii': 'iii', 'iu': 'iku', 'ie': 'ile', 'ia': 'ina', 
    'id': 'ind', 'ik': 'ipk', 'is': 'isl', 'it': 'ita', 'jv': 'jav', 
    'ja': 'jpn', 'kl': 'kal', 'kn': 'kan', 'ks': 'kas', 'ka': 'kat', 
    'kr': 'kau', 'kk': 'kaz', 'km': 'khm', 'ki': 'kik', 'rw': 'kin', 
    'ky': 'kir', 'kv': 'kom', 'kg': 'kon', 'ko': 'kor', 'kj': 'kua', 
    'ku': 'kur', 'lo': 'lao', 'la': 'lat', 'lv': 'lav', 'li': 'lim', 
    'ln': 'lin', 'lt': 'lit', 'lb': 'ltz', 'lu': 'lub', 'lg': 'lug', 
    'mh': 'mah', 'ml': 'mal', 'mr': 'mar', 'mk': 'mkd', 'mg': 'mlg', 
    'mt': 'mlt', 'mn': 'mon', 'mi': 'mri', 'ms': 'msa', 'my': 'mya', 
    'na': 'nau', 'nv': 'nav', 'nr': 'nbl', 'nd': 'nde', 'ng': 'ndo', 
    'ne': 'nep', 'nl': 'nld', 'nn': 'nno', 'nb': 'nob', 'no': 'nor', 
    'ny': 'nya', 'oc': 'oci', 'oj': 'oji', 'or': 'ori', 'om': 'orm', 
    'os': 'oss', 'pa': 'pan', 'pi': 'pli', 'pl': 'pol', 'pt': 'por', 
    'ps': 'pus', 'qu': 'que', 'rm': 'roh', 'ro': 'ron', 'rn': 'run', 
    'ru': 'rus', 'sg': 'sag', 'sa': 'san', 'si': 'sin', 'sk': 'slk', 
    'sl': 'slv', 'se': 'sme', 'sm': 'smo', 'sn': 'sna', 'sd': 'snd', 
    'so': 'som', 'st': 'sot', 'es': 'spa', 'sq': 'sqi', 'sc': 'srd', 
    'sr': 'srp', 'ss': 'ssw', 'su': 'sun', 'sw': 'swa', 'sv': 'swe', 
    'ty': 'tah', 'ta': 'tam', 'tt': 'tat', 'te': 'tel', 'tg': 'tgk', 
    'tl': 'tgl', 'th': 'tha', 'ti': 'tir', 'to': 'ton', 'tn': 'tsn', 
    'ts': 'tso', 'tk': 'tuk', 'tr': 'tur', 'tw': 'twi', 'ug': 'uig', 
    'uk': 'ukr', 'ur': 'urd', 'uz': 'uzb', 've': 'ven', 'vi': 'vie', 
    'vo': 'vol', 'wa': 'wln', 'wo': 'wol', 'xh': 'xho', 'yi': 'yid', 
    'yo': 'yor', 'za': 'zha', 'zh': 'zho', 'zu': 'zul', 'zxx': 'zxx'
}
"""Dictionary mapping languages codes ISO639-1 to ISO639-3."""