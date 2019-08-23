# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 18:06:14 2019

@author: zhengyawen104
"""

import pandas as pd
import numpy as np
import os
#from datetime import date, datetime, timedelta
import datetime
import sys
sys.path.append('//paicdom/paamhk/Aurelius/QiuChuchu/BBG')
import BloombergFormula as bbg
import copy
import sys
sys.path.append('//paicdom/paamhk/Aurelius/QiuChuchu/DataUpdate/tools')
from mongodb import tomongo

#Since FX_Vol Data would be downloaded in a csv file and the file is not big, we can directly upload
# set download = True, upload = True

def main(category, endDate, fields,fields_new,collection_name, download = False, upload = False):
    #set the path to store downloaded data by update date
    update_date = datetime.date.today().strftime('%Y%m%d')
    path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)
    # read data out from MongoDB to get tickers and startDate
   #data = query_all(db_name = 'MarketData',collection_name = collection_name)
#    data = query_all(db_name = 'MarketDataUpdate', collection_name = collection_name)
#    #TODO: check if all the tickers have the same startDate to update from 
#    #download data from startDate, so updated data has one day duplicated
#    startDate = data['date'].max()
#    startDate = pd.to_datetime(startDate, unit='s')+pd.Timedelta('1 days')
#    startDate = startDate.strftime('%Y%m%d')
#    
    startDate = '20190627'
    #Lucas:change startdate to one week before today
    #startDate = datetime.strftime(date.today()-timedelta(8),'%Y%m%d')
    
    tickers = pd.read_csv('//paicdom/paamhk/Aurelius/QiuChuchu/MarketData Downloading/FX_Vol/fx_vol_tickers.csv')
    ##get the earlist start date as the input startDate for getDataframe
    ticker_list=list(tickers['ticker'])
    if download:
        date=bbg.BBGFormula(ticker_list).getStartDate()    
        date=date[date['start_date']!='error']
        date=date.reset_index(drop=True)
        date.start_date=date.start_date.apply(lambda x : pd.to_datetime(x, yearfirst = True).strftime('%Y%m%d'))
            
        ticker_list= list(date['ticker'])
        
        fdata,invalidSec=bbg.BBGFormula(ticker_list).multi_ticker_multi_fields_BDH(fields,startDate,endDate)
        
        data_mongo=pd.merge(fdata,tickers,how='left',on='ticker')   
        
        old = copy.deepcopy(fields)
        new = copy.deepcopy(fields_new)
        data_mongo.rename(columns=dict(zip(old,new)), inplace=True)
        #value is the mid price
        #data_mongo['value'] = 0.5*[data_mongo['ask'] + data_mongo['bid'] ]
        #Lucas: Not suitable when value is a list
        def f(x):
            return 0.5*(x['ask']+x['bid'])
        data_mongo['value'] = data_mongo.apply(f,axis = 1)
        data_mongo['date'] = data_mongo['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst = True).value // 1e9))
        data_mongo.to_csv(path + '/' + ticker + collection_name + '.csv',index=False)
    if upload:
        tomongo(data_mongo, collection_name = collection_name, to_mongo =False, replace = False, db_name = 'MarketDataUpdate')
        


if __name__ == '__main__':
    category = 'FX_Vol'
    #Lucas:change endDate to yesterday.
    #endDate = datetime.strftime(date.today()-timedelta(1),'%Y%m%d')
    endDate = '20190816'
    collection_name = 'FX_Vol'
    fields = ['PX_BID','PX_ASK']
    #fields' name in MongoDB
    fields_new =  ['bid','ask']
    #Download no adjusted prices for only one ticker
    
    main(category, endDate, fields,fields_new, collection_name, download = True , upload = True)
