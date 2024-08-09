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


def main():

    email = 'mackenzie.mcdonald@agr.gc.ca'
    name = infer_name_from_email(email)
    print(name)

if __name__ == '__main__':
    main()