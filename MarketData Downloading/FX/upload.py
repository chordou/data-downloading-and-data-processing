import pandas as pd
import numpy as np
import os
import json
import pymongo



ip = '10.63.16.76'

def UploadCsvFileToMongo(filepath, collection_name, to_mongo=False, replace=False, db_name='MarketDataUpdate'):
    data = pd.read_csv(filepath, dtype = {'name' : str}) # force data['name'] change to string
    data_json = json.loads(data.to_json(orient='records'))
    if to_mongo:
        mng_client = pymongo.MongoClient(ip, 27017)
        mng_db = mng_client[db_name]
        db_cm = mng_db[collection_name]
        if replace:
            db_cm.delete_many({})
        print("uploading data to MongoDB...")
        db_cm.insert_many(data_json)
        print("upload is finished")
        mng_client.close()



if __name__ == '__main__':

    path = r'C:\Users\QIUCHUCHU428\PycharmProjects\test\FX\data_mongo'
    for name in os.listdir(path):
       print(name)
       filepath = os.path.join(path, name)
       UploadCsvFileToMongo(filepath, collection_name = 'FX', to_mongo=True, replace=False, db_name='MarketDataUpdate')
