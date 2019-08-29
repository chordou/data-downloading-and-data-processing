# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 18:06:14 2019

@author: zhengyawen104
"""

import pandas as pd
import numpy as np
import os
import datetime
from BBG import BloombergFormula as bbg
from DataUpdate.tools.mongodb import query_all, query_part,tomongo
import copy


#Download adjusted prices and volume for only one ticker
#Note: If download adjusted prices from BBG directly, we should download from the earlist date

def main(category, endDate, tickers, collection_name, download = False, upload = False):
    #set the path to store downloaded data by update date
    update_date = datetime.date.today().strftime('%Y%m%d')
    path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)
    
    
    assert len(tickers) == 4
    ticker = tickers[0]
    name = ticker[:ticker.find(" ")]
    startDate = "19000101"
    if download:
        #download data from bloomberg and store each ticker's data as a csv file 
        
        dataTFF,invalidSec=bbg.BBGFormula(ticker).adjust_sigle_ticker_BDH(fields,startDate,endDate,split=True, adjustNormal=False,adjustAbnormal=False)
        dataTFF.rename(columns={'PX_LAST':'close_split_only','PX_OPEN':'open_split_only','PX_HIGH':'high_split_only','PX_LOW':'low_split_only','PX_VOLUME':'volume_split'},inplace=True)
#Because volume only has split adjustment, so all adjust will not influence volume
#So we don't have to download all adjusted volume 
        fields.remove("PX_VOLUME")
        dataTTT,invalidSec=bbg.BBGFormula(ticker).adjust_sigle_ticker_BDH(fields,startDate,endDate,split=True, adjustNormal=True,adjustAbnormal=True)
        dataTTT.rename(columns={'PX_LAST':'close_threeAdj','PX_OPEN':'open_threeAdj','PX_HIGH':'high_threeAdj','PX_LOW':'low_threeAdj'},inplace=True)
        
        dataTTT = dataTTT[list(set(dataTTT.columns) - set(["ticker"]))]
        dataa = pd.merge(dataTFF, dataTTT, on = 'date', how = 'outer')
        dataa.to_csv(path+"/"+ticker+'.csv',index=False)
        #1) upload downloaded csv file of adjusted price&volume
                
        data_mongo = pd.DataFrame()
        for file in os.listdir(path):
            dataone = pd.read_csv(os.path.join(path, file))
            dataone['date'] = dataone['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst = True).value // 1e9))
            for col in list(set(dataone.columns) -set(['date','ticker'])):
#                print(col)
                part = dataone[['date','ticker',col]]
                part = part.dropna(axis = 0, how = 'any')
                part.rename(columns = {col: 'value'}, inplace  =True)        
                part.insert(len(part.columns), 'name',file[:file.find(" ")],True)
                part.insert(len(part.columns), 'tenor','spot',True)  
                if col.find("split") < 0:
                    part.insert(len(part.columns), 'adjust','all',True)
                if col.find("split") >= 0:
                    part.insert(len(part.columns), 'adjust','split',True)
                part.insert(len(part.columns), 'feature',col[:col.find("_")],True)
                data_mongo = data_mongo.append(part, ignore_index = True)    
        
        assert len(set(data_mongo['name'])) == len(os.listdir(path))
        
        # 1) upload updated data without adjusted to 'MarketDataUpdate'
    if upload:
        tomongo(data_mongo, collection_name = collection_name, to_mongo =False, replace = False, db_name = 'MarketDataUpdate')
        


if __name__ == '__main__':
    category = 'STOCKS_ADJ'
    endDate = '20190816'
    collection_name = 'HKEX_ADJ'
    fields = ["PX_LAST","PX_OPEN","PX_HIGH","PX_LOW","PX_VOLUME"]
    #fields' name in MongoDB
    fields_new =  ['close', 'open','high', 'low','volume']
    tickers = ["3690 HK Equity","788 HK Equity", "2359 HK Equity","1765 HK Equity"]
    main(category, endDate, tickers, collection_name, download = True, upload = False)
