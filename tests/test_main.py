"""This code tests the main function(s) data_cleaner/__main__.py is intended 
to be run from project's top folder using:
  py -m unittest tests.test_main
"""

from data_scanner.__main__ import *

import unittest


class TestMain(unittest.TestCase):

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
        

    def test_add_dataset(self):

        # datasets fetched for testing are noted as: frequency:"not planned"
        # hence they should not change with time and tests should keep working

        registry = DataCatalogue(REGISTRY_BASE_URL)
        datasets_cols = ['id', 'title_en', 'title_fr', 'date_published', 
                    'metadata_created', 'metadata_modified' , 
                    'num_resources', 'author_email', 'maintainer_email']
        lock = threading.Lock()
        
        id1 = 'b8cfc949-501a-4c04-a265-bfe844f41972'
        expected1 = pd.DataFrame(columns=datasets_cols)
        expected1.loc[2] = {'id': id1,
                            'title_en': '2006 Canada-US Softwood Lumber Agreement Softwood Lumber Exports Report - 2012',
                            'title_fr': "Accord De 2006 sur le Bois D'œuvre Résineux Entre le Canada et les États-Unis Rapport sur les Exportations de Bois D'œuvre Résineux - 2012",
                            'date_published': '2015-08-24 00:00:00',
                            'metadata_created': '2024-07-22T16:00:19.806466',
                            'metadata_modified': '2024-07-22T16:00:19.806470',
                            'num_resources': 8,
                            'author_email': None,
                            'maintainer_email': 'open-ouvert@tbs-sct.gc.ca'
                            }
        result1 = pd.DataFrame(columns=datasets_cols)
        add_dataset(registry, id1, result1, 2, lock)
        self.assertTrue(result1.compare(expected1).empty)

        id2 = 'de1f8f2a-2899-4469-8545-f247c717d112'
        expected2 = pd.DataFrame(columns=datasets_cols)
        expected2.loc[12] = {'id': id2,
                            'title_en': 'Reducing pest risk in birch wood products - The effective heat treatment for bronze birch borer Agrilus anxius (Coleoptera: Buprestidae) prepupae',
                            'title_fr': "Réduire le risque des ravageurs dans les produits du bois – Traitements à la chaleur efficace pour les prépupes de l’agrile du bouleau, Agrilus anxius (Coleoptera : Buprestidae)",
                            'date_published': '2024-07-19 00:00:00',
                            'metadata_created': '2024-07-22T13:10:18.645273',
                            'metadata_modified': '2024-07-22T13:10:18.645279',
                            'num_resources': 3,
                            'author_email': None,
                            'maintainer_email': 'Christian.MacQuarrie@nrcan-rncan.gc.ca'
                            }
        result2 = pd.DataFrame(columns=datasets_cols)
        add_dataset(registry, id2, result2, 12, lock)
        self.assertTrue(result2.compare(expected2).empty)
        

    def test_main(self):
        pass            # TO BE IMPLEMENTED

if __name__ == '__main__':
    unittest.main()