"""This code tests the DataCatalogue class implemented in __main__.py and is 
intended to be run from project's top folder using:
  py -m unittest tests.test_data_catalogue
Use -v for more verbose.
"""

from aafc_data_scanner.constants import *
from aafc_data_scanner.tools import *
from aafc_data_scanner.inventories import *

import numpy as np
import unittest


class TestDataCatalogue(unittest.TestCase):

    # helper function
    def assert_and_see_differences(self,
                                  df1: pd.DataFrame, 
                                  df2: pd.DataFrame) -> bool:
        """Asserts both given dataframes are the same; if not, returns
        False and prints differences.
        """
        if not np.array_equal(df1.columns.values, df2.columns.values):
            print('Difference found between result and expectation columns:')
            print(f'df1: {df1.columns.values}')
            print(f'df2: {df2.columns.values}')
        if not np.array_equal(df1.index.values, df2.index.values):
            print('Difference found between result and expectation indices:')
            print(f'df1: {df1.index.values}')
            print(f'df2: {df2.index.values}')
        validated = df1.compare(df2).empty
        if not validated:
            print('Difference found between result and expectation:\n')
            if 'id' in df1.columns:
                print(f'id: {df1.loc[0, 'id']}')
            print(df1.compare(df2))
        self.assertTrue(validated)
        

    def test_add_dataset(self):

        # datasets fetched for testing are noted as: frequency:"not planned"
        # hence they should not change with time and tests should keep working
        inventory = Inventory()
        registry = RequestsDataCatalogue(REGISTRY_BASE_URL)
        lock = threading.Lock()
        test_datasets = pd.read_csv('tests/io/datasets.csv', 
                                    encoding='utf-8-sig')
        
        for i in range(len(test_datasets)):
            id = test_datasets.loc[i, 'id']
            dataset = registry.get_dataset(id)
            expected = test_datasets.loc[[i]].copy().reset_index(drop=True)
            actual = pd.DataFrame(columns=DATASETS_COLS)
            inventory.add_dataset(dataset, actual, lock)
            self.assert_and_see_differences(actual, expected)
        
    def test_add_resource(self):

        session = TenaciousSession()
        registry = RequestsDataCatalogue(REGISTRY_BASE_URL, session)
        lock = threading.Lock()
        test_resources = pd.read_csv('tests/io/resources.csv', 
                                     encoding='utf-8-sig')

        for i in range(len(test_resources)):
            id = test_resources.loc[i, 'id']
            resource = registry.get_resource(id)
            expected = test_resources.loc[[i]].copy()
            expected.reset_index(drop=True, inplace=True)
            actual = pd.DataFrame(columns=RESOURCES_COLS)
            Inventory.add_resource(resource, actual, lock)
            # to avoid issues with 200/302 when testing
            # result['url'] = None 
            self.assert_and_see_differences(actual, expected)

    def test_get_modified(self):

        datasets = pd.read_csv('tests/io/datasets_modified.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_modified.csv', 
                                encoding='utf-8-sig')

        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_modified']
            actual = Inventory.infer_modified(ds, resources)
            if actual != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {actual}')
                print(f'Expected: {expected}\n')
            self.assertEqual(actual, expected)

    def test_get_up_to_date(self):

        datasets = pd.read_csv('tests/io/datasets_up_to_date.csv', 
                               encoding='utf-8-sig')

        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            now = dt.datetime.fromisoformat(dataset['now'])
            expected: bool = dataset['expected_up_to_date']
            if ds['id'] == 'b8cfc949-501a-4c04-a265-bfe844f41979':
                expected = None
                continue    # remove continue to test issue message
                            # when reading unreadable frequencies
            actual = Inventory.get_up_to_date(ds, now=now)
            if actual != expected:
                print(f'\nDifference for id #{ds['id']}:')
                print(f'Result: {actual}')
                print(f'Expected: {expected}\n')
            self.assertEqual(actual, expected)
    
    def test_get_official_lang(self):

        datasets = pd.read_csv('tests/io/datasets_official_lang.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_official_lang.csv', 
                                encoding='utf-8-sig')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_ol']
            actual = Inventory.get_official_lang(ds, resources)
            if actual != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {actual}')
                print(f'Expected: {expected}\n')
            self.assertEqual(actual, expected)
    
    def test_get_open_formats(self):

        datasets = pd.read_csv('tests/io/datasets_open_formats.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_open_formats.csv', 
                                encoding='utf-8-sig')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_open_formats']
            actual = Inventory.get_open_formats(ds, resources)
            if actual != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {actual}')
                print(f'Expected: {expected}\n')
            self.assertEqual(actual, expected)

    def test_get_spec(self):

        datasets = pd.read_csv('tests/io/datasets_spec.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_spec.csv', 
                                encoding='utf-8-sig')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_spec']
            actual = Inventory.get_spec(ds, resources)
            if actual != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {actual}')
                print(f'Expected: {expected}\n')
            self.assertEqual(actual, expected)

    def test_complete_modified(self):
        
        datasets = pd.read_csv('tests/io/datasets_modified.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_modified.csv', 
                                encoding='utf-8-sig')

        pd.options.mode.chained_assignment = None 
        # to avoid SettingWithCopyWarning
        expected = datasets[DATASETS_COLS]
        expected['modified'] = datasets['expected_modified'].copy()

        actual = Inventory()
        actual.datasets = datasets[DATASETS_COLS]
        actual.resources = resources[RESOURCES_COLS]
        actual.complete_modified()

        self.assert_and_see_differences(actual.datasets,
                                        expected)

    def test_complete_up_to_date(self):
        
        datasets = pd.read_csv('tests/io/datasets_up_to_date.csv', 
                               encoding='utf-8-sig')

        pd.options.mode.chained_assignment = None 
        # to avoid SettingWithCopyWarning
        expected = datasets[DATASETS_COLS]
        expected['up_to_date'] = datasets['expected_up_to_date1'].copy()
        expected.drop(index=len(expected)-1, inplace=True)

        actual = Inventory()
        actual.datasets = datasets[DATASETS_COLS]
        actual.datasets.drop(index=len(actual.datasets)-1, inplace=True)
        now = dt.datetime(2023, 6, 1)
        actual.complete_up_to_date(now)

        self.assert_and_see_differences(actual.datasets,
                                        expected)

    def test_complete_official_lang(self):

        datasets = pd.read_csv('tests/io/datasets_official_lang.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_official_lang.csv', 
                                encoding='utf-8-sig')

        pd.options.mode.chained_assignment = None 
        # to avoid SettingWithCopyWarning
        expected = datasets[DATASETS_COLS]
        expected['official_lang'] = datasets['expected_ol'].copy()

        actual = Inventory()
        actual.datasets = datasets[DATASETS_COLS]
        actual.resources = resources[RESOURCES_COLS]
        actual.complete_official_lang()

        self.assert_and_see_differences(actual.datasets,
                                        expected)
    
    def test_complete_open_formats(self):
        
        datasets = pd.read_csv('tests/io/datasets_open_formats.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_open_formats.csv', 
                                encoding='utf-8-sig')

        pd.options.mode.chained_assignment = None 
        # to avoid SettingWithCopyWarning
        expected = datasets[DATASETS_COLS]
        expected['open_formats'] = datasets['expected_open_formats'].copy()

        actual = Inventory()
        actual.datasets = datasets[DATASETS_COLS]
        actual.resources = resources[RESOURCES_COLS]
        actual.complete_open_formats()

        self.assert_and_see_differences(actual.datasets,
                                        expected)
    
    def test_complete_spec(self):
        
        datasets = pd.read_csv('tests/io/datasets_spec.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources_spec.csv', 
                                encoding='utf-8-sig')

        pd.options.mode.chained_assignment = None 
        # to avoid SettingWithCopyWarning
        expected = datasets[DATASETS_COLS]
        expected['spec'] = datasets['expected_spec'].copy()

        actual = Inventory()
        actual.datasets = datasets[DATASETS_COLS]
        actual.resources = resources[RESOURCES_COLS]
        actual.complete_spec()

        self.assert_and_see_differences(actual.datasets,
                                        expected)

    # UPDATE THIS ONE TO REFLECT _collect_dataset_with_resources
    def test_collect_dataset_with_resources(self):

        inventory = Inventory()
        dc = RequestsDataCatalogue(REGISTRY_BASE_URL)
        datasets_lock = threading.Lock()
        resources_IDs_lock = threading.Lock()
        datasets = pd.read_csv('tests/io/datasets.csv', 
                               encoding='utf-8-sig')
        resources = pd.read_csv('tests/io/resources.csv',
                                encoding='utf-8-sig')

        for i in range(len(datasets)):
            id = datasets.loc[i, 'id']
            inventory._collect_dataset_with_resources(
                dc, id, datasets_lock, resources_IDs_lock)
        self.assert_and_see_differences(inventory.datasets, 
                                        datasets)
        self.assert_and_see_differences(inventory.resources, 
                                        resources)
        
    def test_update_platform_info(self):
        pass            # TODO

    # there is no test_inventory
    # no way to plan the whole expected output other than by the code itself


if __name__ == '__main__':
    unittest.main()