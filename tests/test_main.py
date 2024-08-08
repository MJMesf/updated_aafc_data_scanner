"""This code tests the functions in data_cleaner/__main__.py 
It is intended to be run from project's top folder using:
  py -m unittest tests.test_main
"""

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

    def test_infer_name_from_email(self):
        pass            # TO BE IMPLEMENTED

    def test_get_modified(self):
        pass            # TO BE IMPLEMENTED

    def test_check_currency(self):

        datasets = pd.read_json('.\\test_files\\datasets_needs_update.json', 
                            dtype={"expected_up_to_date":bool})
        resources = pd.read_json('.\\test_files\\resources_needs_update.json')

        for _, dataset in datasets.iterrows():
            ds = dataset[DATASETS_COLS]
            if ds['id'] == 'b8cfc949-501a-4c04-a265-bfe844f41979':
                ds['expected_up_to-date'] = None
            now = dt.datetime.strptime(dataset['now'], "%Y-%m-%d %H:%M:%S")
            expected = dataset['expected_up_to_date']
            if ds['id'] == 'b8cfc949-501a-4c04-a265-bfe844f41979':
                expected = None
                continue    # remove continue to test issue message
                            # when reading unreadable frequencies
            result = check_currency(ds, resources, now=now)
            self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()