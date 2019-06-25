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



def main(category, endDate, fields,fields_new, tickers, collection_name,startDate = None, download = False, upload = False):
    #set the path to store downloaded data by update date
    update_date = datetime.date.today().strftime('%Y%m%d')
    path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)
    
    # read data out from MongoDB to get tickers and startDate
    assert len(tickers) == 1
    ticker = tickers[0]
    name = ticker[:ticker.find(" ")]
    if startDate is None:        
        data = query_all(db_name = 'MarketData',collection_name = collection_name, name = name)
        
        #download data from startDate, so updated data has one day duplicated
        startDate = data['date'].max()
        startDate = pd.to_datetime(startDate, unit='s').strftime('%Y%m%d')
    #TODO: check if the downloaded data and updated data have same value on the startDate
    
    if download:
        #download data from bloomberg and store each ticker's data as a csv file 
        # for no adjusted data, use adjust_sigle_ticker_BDH

        dataFFF,invalidSec=bbg.BBGFormula(ticker).adjust_sigle_ticker_BDH(fields,startDate,endDate,split=False, adjustNormal=False,adjustAbnormal=False)
        dataFFF.to_csv(path+"/"+ticker+'.csv',index=False)

        #1) upload downloaded csv file of no adjusted price&dividend
        
        old = copy.deepcopy(fields)
        new = copy.deepcopy(fields_new)
        
        data_mongo = pd.DataFrame()
        for file in os.listdir(path):
            dataone = pd.read_csv(os.path.join(path, file))
            dataone.rename(columns=dict(zip(old,new)), inplace=True) 
            dataone['date'] = dataone['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst = True).value // 1e9))
            for col in list(set(dataone.columns) -set(['date','ticker'])):
                part = dataone[['date',col]]
                part = part.dropna(axis = 0, how = 'any')
                part.rename(columns = {col: 'value'}, inplace  =True)        
                part.insert(len(part.columns), 'name',file[:file.find(" ")],True)
                part.insert(len(part.columns), 'tenor','spot',True)           
                part.insert(len(part.columns), 'adjust','no',True)
                part.insert(len(part.columns), 'feature',col,True)
                data_mongo = data_mongo.append(part, ignore_index = True)    
        
        assert len(set(data_mongo['name'])) == len(os.listdir(path))
        
        # 1) upload updated data without adjusted to 'MarketDataUpdate'
        if upload:
            tomongo(data_mongo, collection_name = collection_name, to_mongo =True, replace = False, db_name = 'MarketDataUpdate')
        


if __name__ == '__main__':
    import time
    category = 'STOCKS_NO_ADJ'
    endDate = '20190320'
    collection_name = 'HKEX_ALL_NOADJ'
    fields = ["PX_LAST","PX_OPEN","PX_HIGH","PX_LOW","PX_VOLUME","TURNOVER", 
              "SHORT_SELL_NUM_SHARES"	,"SHORT_SELL_TURNOVER","CUR_MKT_CAP"]
    #fields' name in MongoDB
    fields_new =  ['close', 'open','high', 'low','volume', 'turnover', 'volume_short_sale',  'turnover_short_sale', 'mkt_cap']
    #Download no adjusted prices for only one ticker
    tickers = ["700 HK Equity"]
    main(category, endDate, fields,fields_new, tickers, collection_name,startDate = "20180706", download = False, upload = False)
    #download data for the tickers in this excel
    universe = pd.read_csv('//paicdom/paamhk/Aurelius/Bohui/HongKongExchangeStocks/universe1025.csv')
    i = 0
    while i!= universe.shape[0]:
        try:  
            tickers = [universe.tickers[i]]
            main(category, endDate, fields,fields_new, tickers, collection_name,startDate = "20180706", download = False, upload = False)
            i += 1
        except bbg.NotFoundError:
            #because sometimes if we download data from BBG too frequently, there may encounter connection errors
            #so we need to waite for a few seconds
            time.sleep(120)
            