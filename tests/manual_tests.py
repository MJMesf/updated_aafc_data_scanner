from data_scanner.__main__ import *

import requests


def main():
    
    registry = DataCatalogue(REGISTRY_BASE_URL)

    # aafc_odc = DataCatalogue(AAFC_ODC_BASE_URL)
    # print(aafc_odc.list_datasets()[:10])

    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')

    datasets = registry.list_datasets()
    print(datasets[:10])
    fst = registry.get_dataset(datasets[0])
    print(fst)


if __name__ == '__main__':
    main()