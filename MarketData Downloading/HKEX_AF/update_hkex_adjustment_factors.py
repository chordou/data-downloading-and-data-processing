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
import time


#This file download adjustment factors for stocks 
#and store them into a new collection because the format is different from stocks


def main(category,tickers, collection_name, download = False, upload = False):
    #set the path to store downloaded data by update date
    update_date = datetime.date.today().strftime('%Y%m%d')
    path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)

    if download:
        #download data from bloomberg and store each ticker's data as a csv file 
        ##download adjustment data 
        adj_normal,invalidSec=bbg.BBGFormula(tickers).getAdjFactor(adj_type='NORMAL_CASH')
        adj_abnormal,invalidSec=bbg.BBGFormula(tickers).getAdjFactor(adj_type='ABNORMAL_CASH')
        adj_split,invalidSec=bbg.BBGFormula(tickers).getAdjFactor(adj_type='CAPITAL_CHANGE')
        adj=pd.DataFrame()
        adj=adj.append(adj_split,ignore_index=True)
        adj=adj.append(adj_abnormal, ignore_index=True)
        adj=adj.append(adj_normal, ignore_index=True)   
        adj.insert(len(adj.columns),"update_date",update_date,True)
        adj.to_csv(path+'/AF.csv', mode='a',index=False)



    if upload:
#upload downloaded csv         
        data_mongo = copy.deepcopy(adj)
        data_mongo['date'] = data_mongo['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst = True).value // 1e9))
        # content = open(download_path).read()
        # length = len(content)
        # if length == 1:
        #     print('no new date')
        # else:
        tomongo(data_mongo, collection_name = collection_name, to_mongo =False, replace = False, db_name = 'MarketDataUpdate')


if __name__ == '__main__':
    category = 'HKEX_AF'
    universe = pd.read_csv('//paicdom/paamhk/Aurelius/Bohui/HongKongExchangeStocks/universe1025.csv')
    collection_name = "HKEX_AF"
    i = 0
    while i!= universe.shape[0]:
        try:  
            tickers = [universe.ticker[i]]
            main(category,tickers, collection_name, download = True, upload = False)
            i += 1
        except bbg.NotFoundError:
            #because sometimes if we download data from BBG too frequently, there may encounter connection errors
            #so we need to waite for a few seconds
            time.sleep(120)
    #main(category,tickers, collection_name, download = False)