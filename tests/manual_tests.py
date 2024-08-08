from data_scanner.__main__ import *
from data_scanner.inventory_functions import *

import datetime as dt
import numpy as np
import re
import json
import warnings

    # tests for the AAFC Open Data Catalogue authentication:
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')


def main():

    # datasets = pd.read_json('.\\test_files\\datasets_needs_update.json', 
    #                         dtype={"expected_up_to_date":bool})
    # resources = pd.read_json('.\\test_files\\resources_needs_update.json')
    # datasets = datasets[DATASETS_COLS]
    # datasets['up_to_date'] = datasets.apply(lambda x: check_currency(x, resources), axis=1)
    # print(datasets)

    # df = pd.DataFrame(columns=DATASETS_COLS)
    # print(df)
    # rec = {'id': 'idgcreu3yc3322ovfn', 'published': '2024-07-12 14:03:12'}
    # df.loc[len(df)] = rec
    # print(df)


    url1 = 'https://open.canada.ca/data/en/dataset/41f33e82-c19c-481e-8873-534b5e6d7ebc'
    url2 = 'https://data-catalogue-donnees.agr.gc.ca/aafc-open-data/41f33e82-c19c-481e-8873-534b5e6d7ebc'

    result = set_null_if_broken(url2)
    print(result)
    print(get_head_and_retry(result).status_code)



if __name__ == '__main__':
    main()