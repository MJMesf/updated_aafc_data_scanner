from data_scanner.__main__ import *

import datetime as dt
import numpy as np
import re
import json
import warnings


    # tests for the AAFC Open Data Catalogue authentication:
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')

formats = pd.read_csv('./helper_tables/formats.csv')

def main():

    # ds = {}
    # ds['id'] = '036e5978-c82c-46f5-ac6b-158a876dc65a'
    all_resources = pd.read_csv('../inventories/resources_inventory_2024-08-09_142800.csv')

    # resources = all_resources[all_resources['dataset_id'] == ds['id']].copy()
    # resources.drop(index=[1,2,3], inplace=True)
    # global formats

    # resources = resources.merge(formats, how='left', on='format')
    # format_types_openness = resources.groupby('format_type')['open'].unique()
    # for elem in resources.groupby('format_type')['open'].unique():
    #     if True not in elem:
    #         print('Fail')
    # print('Good')

    datasets = pd.read_csv('../inventories/datasets_inventory_2024-08-09_142800.csv')
    datasets['open_formats'] = datasets.apply(lambda ds: get_open_formats(ds, all_resources), axis=1)
    print(datasets.loc[datasets['open_formats'] == False, ['id', 'title_en', 'registry_link']])
    


if __name__ == '__main__':
    main()