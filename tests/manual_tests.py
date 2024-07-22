from data_scanner.__main__ import *

import concurrent.futures
import requests
import time


    # token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJzblN1ZXBHWU1PajhRZ3ByamxpN29oMmczd1BVN3JDYUFEc2NJQnRzaXdFIiwiaWF0IjoxNzIxNDE5Mjk1fQ.1OdSWaTyfdQovQXAkdfHcmpwdz73wGbTsIfAv8wQI7g'
    # url = 'https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list'
    # test = requests.get('https://data-catalogue-donnees.agr.gc.ca/api/action/organization_list')

def main():
    
    registry = DataCatalogue(REGISTRY_BASE_URL)

    datasets_cols = ['id', 'title_en', 'title_fr', 'date_published', 
                     'metadata_created', 'metadata_modified' , 
                     'num_resources', 'author_email', 'maintainer_email']
    datasets = pd.DataFrame(columns=datasets_cols)

    # Listing IDs of all datasets published by AAFC

    start = time.time()
    ids = registry.search_datasets(owner_org=AAFC_ORG_ID)
    end = time.time()
    print(f'Time for request: {end-start}')

    start = time.time()


    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(len(ids)):
            executor.submit(add_dataset, registry, ids[i], datasets, i)
    end = time.time()
    print(f'Time for entering data in the dataframe: {end-start}')

    print(datasets.loc[:20])

if __name__ == '__main__':
    main()