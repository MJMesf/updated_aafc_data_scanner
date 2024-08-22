"""This code tests the functions in data_cleaner/__main__.py 
It is intended to be run from project's top folder using:
  py -m unittest tests.test_main
"""

from data_scanner.constants import *
from data_scanner.__main__ import *

import unittest

class TestMain(unittest.TestCase):

    def test_get_modified(self):

        datasets = pd.read_json('.\\test_files\\datasets_modified.json', 
                                dtype={'expected_modified': str})
        resources = pd.read_json('.\\test_files\\resources_modified.json')

        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_modified']
            result = get_modified(ds, resources)
            if result != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)

    def test_get_currency(self):

        datasets = pd.read_json('.\\test_files\\datasets_currency.json', 
                                dtype={'modified': str})

        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            now = dt.datetime.strptime(dataset['now'], "%Y-%m-%d %H:%M:%S")
            expected = dataset['expected_currency']
            if ds['id'] == 'b8cfc949-501a-4c04-a265-bfe844f41979':
                expected = None
                continue    # remove continue to test issue message
                            # when reading unreadable frequencies
            result = get_currency(ds, now=now)
            if result != expected:
                print(f'\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)
    
    def test_get_official_langs(self):

        datasets = pd.read_json('.\\test_files\\datasets_ol.json')
        resources = pd.read_json('.\\test_files\\resources_ol.json')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_ol']
            result = get_official_langs(ds, resources)
            if result != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)
    
    def test_get_open_formats(self):

        datasets = pd.read_json('.\\test_files\\datasets_open_formats.json')
        resources = pd.read_json('.\\test_files\\resources_open_formats.json')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_open_formats']
            result = get_open_formats(ds, resources)
            if result != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)

    def test_get_spec_compliance(self):

        datasets = pd.read_json('.\\test_files\\datasets_spec.json')
        resources = pd.read_json('.\\test_files\\resources_spec.json')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_spec_compliance']
            result = get_spec_compliance(ds, resources)
            if result != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()