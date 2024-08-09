"""This code tests the functions in data_cleaner/__main__.py 
It is intended to be run from project's top folder using:
  py -m unittest tests.test_main
"""

from data_scanner.constants import *
from data_scanner.__main__ import *

import unittest

class TestMain(unittest.TestCase):

    # helper function
    def assert_and_see_differences(self,
                                  df1: pd.DataFrame, 
                                  df2: pd.DataFrame) -> bool:
        """Asserts both given dataframes are the same; if not, returns
        False and prints differences.
        """
        validated = df1.compare(df2).empty
        if not validated:
            print('Difference found between result and expectation:\n')
            if 'url' in df1.columns:
                print(f'url: {df1.loc[0, 'url']}')
            print(df1.compare(df2))
        self.assertTrue(validated)

    def test_request(self):

        url1 = REGISTRY_BASE_URL + 'package_list'
        result1 = request(url1)
        self.assertIsInstance(result1, list)

        url2 = REGISTRY_BASE_URL + 'package_lists'
        with self.assertRaises(AssertionError) as ae2:
            result2 = request(url2)
        self.assertEqual(str(ae2.exception), 
                         'Request Error:\nUnexpected status code: 400')

        url3 = 'https://open.canada.ca/data/thisisatest'
        with self.assertRaises(AssertionError) as ae3:
            result3 = request(url3)
        self.assertEqual(str(ae3.exception), 
                         'Request Error:\nUnexpected status code: 404')

        url4 = REGISTRY_BASE_URL + 'dashboard_activity_list'
        with self.assertRaises(AssertionError) as ae4:
            result4 = request(url4)
        self.assertEqual(str(ae4.exception), 
                         'Request Error:\nUnexpected status code: 403')


    # INVENTORYING FUNCTIONS *************************************************


    def test_add_dataset(self):

        # datasets fetched for testing are noted as: frequency:"not planned"
        # hence they should not change with time and tests should keep working

        registry = DataCatalogue(REGISTRY_BASE_URL)
        lock = threading.Lock()
        test_datasets = pd.read_json('.\\test_files\\datasets1.json')

        for i in range(len(test_datasets)):
            id = test_datasets.loc[i, 'id']
            dataset = registry.get_dataset(id)
            expected = test_datasets.loc[[i]].copy().reset_index(drop=True)
            result = pd.DataFrame(columns=DATASETS_COLS)
            add_dataset(dataset, result, lock)
            self.assert_and_see_differences(result, expected)
        
    def test_add_resource_ID(self):

        registry = DataCatalogue(REGISTRY_BASE_URL)
        resources_IDs = ['3727a6b2-c056-4dd1-b578-6886bdea67f4',
                         '1dd7a9b9-fbe9-480a-b1f3-45a2fa9a5460',
                         '4df4b259-3042-4575-8925-c58a1587eb29']
        lock = threading.Lock()
        
        id1 = '5f3e5014-3154-4526-a0f2-62f14fe5df60'
        dataset1 = registry.get_dataset(id1)
        resources_IDs1 = ['f928cf11-308f-481b-9d4f-d6873aa81480',
                          '6c243d9f-5a12-4944-8bbf-9365adddd6cf',
                          '6e92b0fb-0abf-4bf7-94da-2bdbe6c76ef2'] 
        expected1 = resources_IDs.copy() 
        expected1.extend(resources_IDs1)
        result1 = resources_IDs.copy()
        add_resource_ID(dataset1, result1, lock)
        self.assertEqual(result1, expected1)

        id2 = '5564fc0b-3fe1-4214-9690-af228d15a93f'
        dataset2 = registry.get_dataset(id2)
        resources_IDs2 = ['63502d2c-1222-4b3a-8d6f-91208a2c590c',
                          'c6b8ca24-8ef0-41d5-9339-6b25c1fb66e8',
                          '56b09b31-e31b-4089-bb24-e914f4b165ca',
                          'b02c6e14-6383-4c63-8ae9-68f483215230',
                          'bb48a081-7a0f-48af-8586-44a8e866816b'] 
        expected2 = resources_IDs.copy() 
        expected2.extend(resources_IDs2)
        result2 = resources_IDs.copy()
        add_resource_ID(dataset2, result2, lock)
        self.assertEqual(result2, expected2)

    def test_add_resource(self):

        registry = DataCatalogue(REGISTRY_BASE_URL)
        lock = threading.Lock()
        test_resources = pd.read_json('./test_files/resources1.json')

        for i in range(len(test_resources)):
            id = test_resources.loc[i, 'id']
            resource = registry.get_resource(id)
            expected = test_resources.loc[[i]].copy()
            expected.reset_index(drop=True, inplace=True)
            result = pd.DataFrame(columns=RESOURCES_COLS)
            add_resource(resource, result, lock)
            self.assert_and_see_differences(result, expected)


    def test_collect_dataset(self):

        catalogue = DataCatalogue(REGISTRY_BASE_URL)
        datasets_lock = threading.Lock()
        resources_IDs_lock = threading.Lock()
        datasets = pd.read_json('.\\test_files\\datasets1_resourcesIDs.json')

        for i in range(len(datasets)):
            id = datasets.loc[i, 'id']
            expected_datasets = datasets.loc[[i], DATASETS_COLS].copy().reset_index(drop=True)
            expected_resources_IDs = datasets.loc[i, 'resources']
            actual_datasets = pd.DataFrame(columns=DATASETS_COLS)
            actual_resources_IDs = []
            collect_dataset(catalogue, id, actual_datasets, datasets_lock,
                            actual_resources_IDs, resources_IDs_lock)
            self.assert_and_see_differences(actual_datasets, 
                                            expected_datasets)
            self.assertEqual(actual_resources_IDs, expected_resources_IDs)

    def test_collect_resource(self):

        catalogue = DataCatalogue(REGISTRY_BASE_URL)
        resources = pd.read_json('.\\test_files\\resources1.json')
        resources_IDs = list(resources['id'])
        lock = threading.Lock()

        for i in range(len(resources_IDs)):
            id = resources_IDs[i]
            expected_resources = resources.loc[[i]]
            # not testing url status; sometimes 302~200 is unstable
            # expected_resources.drop(columns=['url_status'], inplace=True)
            expected_resources.reset_index(drop=True, inplace=True)
            actual_resources = pd.DataFrame(columns=RESOURCES_COLS)
            collect_resource(catalogue, id, actual_resources, lock)
            # actual_resources.drop(columns=['url_status'], inplace=True)
            self.assert_and_see_differences(actual_resources, 
                                            expected_resources)


    # HELPER FUNCTIONS *******************************************************


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
            result = infer_name_from_email(row['email'])
            expected = row['name']
            self.assertEqual(result, expected)

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

    def test_check_currency(self):

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
            result = check_currency(ds, now=now)
            if result != expected:
                print(f'\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)
    
    def test_check_official_langs(self):

        datasets = pd.read_json('.\\test_files\\datasets_ol.json')
        resources = pd.read_json('.\\test_files\\resources_ol.json')
        
        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            expected = dataset['expected_ol']
            result = check_official_langs(ds, resources)
            if result != expected:
                print(f'\n\nDifference for id #{ds['id']}:')
                print(f'Result: {result}')
                print(f'Expected: {expected}\n')
            self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()