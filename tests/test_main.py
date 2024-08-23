"""This code tests the functions in data_cleaner/__main__.py 
It is intended to be run from project's top folder using:
  py -m unittest tests.test_main
Use -v for more verbose.
"""

from data_scanner.constants import *
from data_scanner.__main__ import *

import unittest

class TestMain(unittest.TestCase):

    def test_date_ago(self):

        tests = pd.read_csv('.\\test_files\\date_ago.csv',
                            parse_dates=['from_', 'expected'])
        for _, row in tests.iterrows():
            n: float = row['n']
            unit: str = row['unit']
            from_: dt.datetime = row['from_'].to_pydatetime()
            expected: dt.datetime = row['expected'].to_pydatetime()
            actual: dt.datetime = date_ago(n, unit, from_)
            self.assertEqual(actual, expected)            

        # Additional tests for invalid inputs

        n1 = -2
        unit1 = 'day'
        from_1 = dt.datetime(2024, 2, 13, 11, 2, 17)
        with self.assertRaises(ValueError) as e1:
            actual1 = date_ago(n1, unit1, from_1)
        self.assertEqual(str(e1.exception), 
                        'Illegal argument (n = -2). n must be >= 0')

        n2 = 1
        unit2 = 'quarter'
        from_2 = dt.datetime(2024, 2, 13, 11, 2, 17)
        with self.assertRaises(ValueError) as e2:
            actual1 = date_ago(n2, unit2, from_2)
        self.assertEqual(str(e2.exception), 
                        'Illegal argument (unit = quarter). Allowed values are day, week, month and year.')

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

    def test_get_up_to_date(self):

        datasets = pd.read_json('.\\test_files\\datasets_currency.json', 
                                dtype={'modified': str})

        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            now = dt.datetime.strptime(dataset['now'], "%Y-%m-%d %H:%M:%S")
            expected = dataset['expected_up_to_date']
            if ds['id'] == 'b8cfc949-501a-4c04-a265-bfe844f41979':
                expected = None
                continue    # remove continue to test issue message
                            # when reading unreadable frequencies
            result = get_up_to_date(ds, now=now)
            if result != expected:
                print(f'\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)
    
    def test_get_official_lang(self):

        datasets = pd.read_json('.\\test_files\\datasets_ol.json')
        resources = pd.read_json('.\\test_files\\resources_ol.json')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_ol']
            result = get_official_lang(ds, resources)
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

    def test_get_spec(self):

        datasets = pd.read_json('.\\test_files\\datasets_spec.json')
        resources = pd.read_json('.\\test_files\\resources_spec.json')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_spec']
            result = get_spec(ds, resources)
            if result != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()