from data_scanner.__main__ import *
from data_scanner.tools import *

import datetime as dt
import numpy as np
import re
import json
import warnings
from colorama import Fore, Style, init, deinit


    # tests for the AAFC Open Data Catalogue authentication:
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')

def main():
    
    # iso639 = pd.read_csv('./helper_tables/iso639_1to3.csv', encoding='utf-8-sig')
    # iso_map = {row['iso639-1']:row['iso639-3'] for _, row in iso639.iterrows()}
    # print(iso_map['fr'])

    tests = pd.read_csv('.\\test_files\\date_ago.csv',
                        parse_dates=['from_', 'expected'])
    # print()
    # print(tests)
    # print()
    # print(tests.dtypes)

    # d = dt.datetime.fromtimestamp(tests.loc[0, 'from_'])
    #d = tests.loc[0, 'from_']
    d = tests.loc[0, 'from_'].to_pydatetime()
    date_ago(-2, 'day', d)
    

if __name__ == '__main__':
    main()