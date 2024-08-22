from data_scanner.__main__ import *

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
    
    msg = '351 datasets and 2560 resources were found.'
    init()
    print('This is a totally pointless message that does not say anything.')
    print(Fore.YELLOW + '351' + Fore.RESET + ' datasets and ' + Fore.YELLOW + '2560' + Fore.RESET + ' resources were found.')
    print('You shall not pass. Fly you fools.')


if __name__ == '__main__':
    main()