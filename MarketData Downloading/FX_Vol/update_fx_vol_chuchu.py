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
from BBG import BloombergFormula as bbg
from DataUpdate.tools.mongodb import query_all, query_part,tomongo
import copy

def main(category, endDate, fields,fields_new, fx_list,expiry_list,expiry_num,option_type, collection_name, download = False, upload = False):
    #set the path to store downloaded data by update date
    update_date = datetime.date.today().strftime('%Y%m%d')
    path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)
    # read data out from MongoDB to get tickers and startDate
   #data = query_all(db_name = 'MarketData',collection_name = collection_name)
    data = query_all(db_name = 'MarketDataUpdate', collection_name = collection_name)
    #TODO: check if all the tickers have the same startDate to update from
    # chuchu: checking the sameDatein findMInDateFXVOL.py file
    #download data from startDate, so updated data has one day duplicated
    startDate = data['date'].max()
    startDate = pd.to_datetime(startDate, unit='s')+pd.Timedelta('1 days')
    startDate = startDate.strftime('%Y%m%d')
    
    #Lucas:change startdate to one week before today
    #startDate = datetime.strftime(date.today()-timedelta(8),'%Y%m%d')
    
    option_list=list(option_type.values())

    # def getTicker(fx,option,expiry,suffix='BGN Curncy'):
    #     if fx =='HKDCNH':
    #         suffix='BLCS Curncy'
    #     ticker=fx+option+expiry+' '+suffix
    #     return ticker
    #
    # tickers=pd.DataFrame(columns=['ticker','name','feature','expiry_str','tenor'])
    # for i in range(len(fx_list)):
    #     for j in range(len(expiry_list)):
    #         for k in range(len(option_list)):
    #             ticker=getTicker(fx_list[i],option_list[k],expiry_list[j],suffix='BGN Curncy')
    #             tickers=tickers.append({'ticker':ticker,'name':fx_list[i],
    #                                     'feature':list(option_type.keys())[k],
    #                                     'expiry_str':expiry_list[j], 'tenor':expiry_num[j]},ignore_index=True)
    #

    old_data = pd.read_csv('/home/pingan/PycharmProjects/test/FX_Vol/dayRangeMarketData.csv')
    startDate = old_data['endDate']+pd.Timedelta('1 days')






    tickers= pd.read_csv('/home/pingan/PycharmProjects/test/FX_Vol/dayRangeMarketDataUpdate.csv')

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
        tomongo(data_mongo, collection_name = collection_name, to_mongo =True, replace = False, db_name = 'MarketDataUpdate')
        


if __name__ == '__main__':
    category = 'FX_Vol'
    #Lucas:change endDate to yesterday.
    #endDate = datetime.strftime(date.today()-timedelta(1),'%Y%m%d')
    endDate = ''
    collection_name = 'FX_Vol'
    fields = ['PX_BID','PX_ASK']
    #fields' name in MongoDB
    fields_new =  ['bid','ask']
    #Download no adjusted prices for only one ticker
    
    fx_list=['CNHJPY','EURCNH','EURHKD','EURJPY','EURUSD','HKDCNH','HKDJPY','USDCNH','USDHKD','USDJPY']
    expiry_list=['ON','1W','2W','3W','1M','2M','3M',	'4M',	'6M',	'9M',	'1Y',	'18M',	'2Y',	'3Y',	'4Y',	'5Y',	'7Y',	'10Y',	'15Y']
    expiry_num=[1,7,14,21,30,60,90,120,180,270,360,540,720,1080,1440,1800,2520,3600,5400]
    option_type={'ATM':'V', '25 RR':'25R', '25 BF':'25B', '10 RR': '10R', '10 BF':'10B'}
    main(category, endDate, fields,fields_new, fx_list,expiry_list,expiry_num,option_type, collection_name, download = True , upload = True)
