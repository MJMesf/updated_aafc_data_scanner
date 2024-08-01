from data_scanner.__main__ import *

import datetime as dt
import numpy as np
import json
import warnings

    # tests for the AAFC Open Data Catalogue authentication:
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')

def main():

    datasets = pd.read_json('.\\test_files\\datasets_needs_update.json', 
                            dtype={"expected_up_to_date":bool})
    resources = pd.read_json('.\\test_files\\resources_needs_update.json')
    datasets = datasets[DATASETS_COLS]
    datasets['up_to_date'] = datasets.apply(lambda x: check_currency(x, resources), axis=1)
    print(datasets)


if __name__ == '__main__':
    main()