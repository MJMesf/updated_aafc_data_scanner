from data_scanner.__main__ import *

import numpy as np
import json
import warnings

    # tests for the AAFC Open Data Catalogue authentication:
    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')

def main():
    
    # resources = pd.read_csv('.\\..\\inventories\\resources_inventory_2024-07-31_155101.csv')
    # resources.rename(columns={'url_status': 'url_status_code'}, inplace=True)

    # resources['url_state'] = resources['url_status_code'].map(
    #     lambda x: 'broken' if (x // 100 == 4 or x == 503) else 'working')
    # url_state_count = resources.groupby('url_state').size().sort_values(ascending=False)
    # summary1 = pd.DataFrame(url_state_count, columns=['count'])
    # summary1['(%)'] = round(summary1['count'] / sum(summary1['count']) * 100, 2)

    # url_status_count = resources.groupby('url_status_code').size().sort_values(ascending=False)
    # summary2 = pd.DataFrame(url_status_count, columns=['count'])
    # summary2['(%)'] = round(summary2['count'] / sum(summary2['count']) * 100, 2)


    # print()
    # print(summary1)
    # print()
    # print(summary2)
    # print()

    url = 'https://agriculture.canada.ca/atlas/data_donnees/biologicalCollections/supportdocument_documentdesupport/fr/Collections biologiques_d_Agriculture_et_Agroalimentaire_Canada_–_SPC_ISO_19131.pdf'
    print(get_head_and_retry(url).status_code)
    print(requests.head(url).status_code)

if __name__ == '__main__':
    main()