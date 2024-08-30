"""This code tests the helper functions and is 
intended to be run from project's top folder using:
  py -m unittest tests.test_data_catalogue
Use -v for more verbose.
"""

from aafc_data_scanner.helper_functions import *

import pandas as pd
import unittest


class TestDataCatalogue(unittest.TestCase):
    
    def test_check_and_create_path(self):
        pass    # TODO

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
    

if __name__ == '__main__':
    unittest.main()