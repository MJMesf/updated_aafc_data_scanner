"""This code tests the DataCatalogue class implementde in __main__.py and is 
intended to be run from project's top folder using:
  py -m unittest tests.test_data_catalogue
"""

from data_scanner.constants import *
from data_scanner.__main__ import *

import unittest


class TestDataCatalogue(unittest.TestCase):

    def test_init(self):

        base_url1 = 'https:/example.com/'
        dc1 = DataCatalogue(base_url1)
        self.assertEqual(dc1.base_url, base_url1)

        base_url2 = ''
        dc2 = DataCatalogue(base_url2)
        self.assertEqual(dc2.base_url, base_url2)

    def test_list_datasets(self):
        registry = DataCatalogue(REGISTRY_BASE_URL)
        datasets = registry.list_datasets()
        self.assertIsInstance(datasets, list)
        self.assertGreaterEqual(len(datasets), 41000)

    def test_get_dataset(self):

        registry = DataCatalogue(REGISTRY_BASE_URL)

        id1 = '000bb94e-d929-4214-8893-bb42b114b0c3'
        title1 = 'COVID-19: How to care at home for someone who has or may have been exposed'
        result1 = registry.get_dataset(id1)
        self.assertEqual(result1['id'], id1)
        self.assertEqual(result1['title'], title1)

        id2 = '3656b2ad-8cf1-4241-8b58-e20154ac2037'
        title2 = 'Notices Softwood Lumber Exports to the United States: Export Allocation Methodologies'
        result2 = registry.get_dataset(id2)
        self.assertEqual(result2['id'], id2)
        self.assertEqual(result2['title'], title2)

    def test_search_datasets(self):

        registry = DataCatalogue(REGISTRY_BASE_URL)
        
        datasets1 = registry.search_datasets(date_published='"2013-03-21%2000:00:00"')
        self.assertEqual(len(datasets1), 4)
        for ds in datasets1:
            record = registry.get_dataset(ds)
            self.assertEqual(record['date_published'], "2013-03-21 00:00:00")


        datasets2 = registry.search_datasets(metadata_created='"2023-03-08T19:28:22.318687Z"')
        self.assertEqual(len(datasets2), 1)
        for ds in datasets2:
            record = registry.get_dataset(ds)
            self.assertEqual(record['metadata_created'], "2023-03-08T19:28:22.318687")
    
    def test_get_resource(self):

        registry = DataCatalogue(REGISTRY_BASE_URL)

        id1 = 'a59fa2be-d0e5-4209-990c-cafe11c7286e'
        name1 = 'Data Product Specification (French)'
        result1 = registry.get_resource(id1)
        self.assertEqual(result1['id'], id1)
        self.assertEqual(result1['name'], name1)

        id2 = '5e09b65b-04ae-4008-80f6-b21be4143703'
        name2 = 'Notices: Item 5203: Sugar-Containing Products Serial No. 166 - 2009-08-20'
        result2 = registry.get_resource(id2)
        self.assertEqual(result2['id'], id2)
        self.assertEqual(result2['name'], name2)
            
if __name__ == '__main__':
    unittest.main()