"""This code tests the DataCatalogue class implementde in __main__.py and is 
intended to be run from project's top folder using:
  py -m unittest tests.test_data_catalogue
Use -v for more verbose.
"""

from data_scanner.constants import *
from data_scanner.tools import *

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


    def test_infer_name_from_email(self):
        df = pd.DataFrame({
            'email': ['jane.doe@agr.gc.ca', 
                      'john-smith@tbs-sct.gc.ca', 
                      'cameron_mackenzie@gmail.com', 
                      'mackenzie.mcdonald@outlook.com'],
            'name': ['Jane Doe', 'John Smith', 
                     'Cameron MacKenzie', 'Mackenzie McDonald']
        })
        for _, row in df.iterrows():
            result = Inventory.infer_name_from_email(row['email'])
            expected = row['name']
            self.assertEqual(result, expected)
        

    def test_add_dataset(self):

        # datasets fetched for testing are noted as: frequency:"not planned"
        # hence they should not change with time and tests should keep working
        inventory = Inventory()
        registry = DataCatalogue(REGISTRY_BASE_URL)
        lock = threading.Lock()
        test_datasets = pd.read_json('.\\test_files\\datasets1.json')

        for i in range(len(test_datasets)):
            id = test_datasets.loc[i, 'id']
            dataset = registry.get_dataset(id)
            expected = test_datasets.loc[[i]].copy().reset_index(drop=True)
            result = pd.DataFrame(columns=DATASETS_COLS)
            inventory.add_dataset(dataset, result, lock)
            self.assert_and_see_differences(result, expected)
        
    def test_add_resource(self):

        session = TenaciousSession()
        inventory = Inventory()
        registry = DataCatalogue(REGISTRY_BASE_URL, session)
        lock = threading.Lock()
        test_resources = pd.read_json('./test_files/resources1.json')

        for i in range(len(test_resources)):
            id = test_resources.loc[i, 'id']
            resource = registry.get_resource(id)
            expected = test_resources.loc[[i]].copy()
            expected.reset_index(drop=True, inplace=True)
            result = pd.DataFrame(columns=RESOURCES_COLS)
            inventory.add_resource(resource, result, lock)
            # to avoid issues with 200/302 when testing
            # result['url'] = None 
            self.assert_and_see_differences(result, expected)

    # UPDATE THIS ONE TO REFLECT _collect_dataset_with_resources
    def test_collect_dataset_with_resources(self):

        inventory = Inventory()
        dc = DataCatalogue(REGISTRY_BASE_URL)
        datasets_lock = threading.Lock()
        resources_IDs_lock = threading.Lock()
        datasets = pd.read_json('.\\test_files\\datasets1.json')
        resources = pd.read_json('.\\test_files\\resources1.json')

        for i in range(len(datasets)):
            id = datasets.loc[i, 'id']
            inventory._collect_dataset_with_resources(
                dc, id, datasets_lock, resources_IDs_lock)
        self.assert_and_see_differences(inventory.datasets, 
                                        datasets)
        self.assert_and_see_differences(inventory.resources, 
                                        resources)

    # there is no test_inventory
    # no way to plan the whole expected output other than by the code itself


if __name__ == '__main__':
    unittest.main()