# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 11:03:15 2019

@author: zhengyawen104
"""

import sys
import pandas as pd
import pymongo
import json

ip ='10.63.16.76.'


def UploadCsvFileToMongo(filepath,collection_name,to_mongo=False,replace=False, db_name ='MarketDataUpdate' ):
    
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)
    data = pd.read_csv(file_res)
    data_json = json.loads(data.to_json(orient='records'))
    if to_mongo:
        mng_client = pymongo.MongoClient(ip, 27017)
        mng_db = mng_client[db_name]
        db_cm = mng_db[collection_name]
        if replace:
            db_cm.delete_many({})   
        print("uploading data to MongoDB...")    
        db_cm.insert_many(data_json)
        print("upload is fished")
        mng_client.close()  
    


if __name__ == "__main__":
  filepath = 'W:\\Data\\DataDownloading\\DataUpdate\\data\\Equity_Vol\\20190627\\HSI IndexEquity_Vol.csv'  
  collection_name = 'Equity_Vol'
  UploadCsvFileToMongo(filepath,collection_name,to_mongo=True,replace=False, db_name ='MarketDataUpdate' )