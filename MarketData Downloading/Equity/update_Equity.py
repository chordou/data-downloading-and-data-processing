# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 18:06:14 2019

@author: zhengyawen104
"""

import pandas as pd
import numpy as np
import os
from datetime import date, datetime, timedelta
from BBG import BloombergFormula as bbg
from DataUpdate.tools.mongodb import query_all, query_part,tomongo
import copy


def main(category, endDate, fields, tickers, download = False, upload = False,change = False, collection_name = 'Equity'):
    #set the path to store downloaded data by update date
    update_date = date.today().strftime('%Y%m%d')
    path = os.path.join('//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/' +category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)
    
    # read data out from MongoDB to get tickers and startDate
    # For index data, the tenor is spot, for index futures, the tenor is future
    #data = query_all(db_name = 'MarketData',collection_name = 'Equity', tenor = 'spot')
    #download data from startDate, so updated data has one day duplicated
    
    #Lucas: solved duplicated date by using timedelta +1day
#    startDate = data['date'].max()
#    startDate = pd.to_datetime(startDate, unit='s')+pd.Timedelta('1 days')
#    startDate = startDate.strftime('%Y%m%d')
    startDate = '20190627'

    
    if download:
        #download data from bloomberg and store each ticker's data as a csv file 
        for i in range(len(tickers)):  
            ticker=tickers[i]  
            data_down,invalidSec=bbg.BBGFormula(ticker).single_ticker_multi_fields_BDH(fields,startDate,endDate)
            data_down.to_csv(path+"/"+ticker+'.csv',index=False)
    
    if change:
        #1) upload downloaded csv file of no adjusted price&dividend
        
        old = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PE_RATIO', 'BEST_PE_RATIO',
               'PX_TO_BOOK_RATIO', 'BEST_PX_BPS_RATIO','DVD_SH_LAST']
        new = ['close','open','high','low','PE','PE_E','PB','PB_E','div']
        
        
        data_mongo = pd.DataFrame()
        for file in os.listdir(path):
            dataone = pd.read_csv(os.path.join(path, file))
            dataone.rename(columns=dict(zip(old,new)), inplace=True) 
            dataone['date'] = dataone['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst = True).value // 1e9))
            for col in list(set(dataone.columns) -set(['date','ticker'])):
                part = dataone[['date', 'ticker',col]]
                part = part.dropna(axis = 0, how = 'any')
                part.rename(columns = {col: 'value'}, inplace  =True)        
                part.insert(3, 'name',file[:file.find(" ")],True)
                part.insert(3, 'tenor','spot',True)
                #DAX Index& IBOV Index are already total return index
                if file[:file.find(" ")] in  ["DAX","IBOV"]:
                    part.insert(3, 'adjust','all',True)  
                else:                
                    part.insert(3, 'adjust','no',True)
                part.insert(3, 'feature',col,True)
                data_mongo = data_mongo.append(part, ignore_index = True)    
        
        assert len(set(data_mongo['ticker'])) == len(os.listdir(path))
        
        # 1) upload updated data without adjusted price to 'MarketDataUpdate'
        if upload:
            tomongo(data_mongo, collection_name = collection_name, to_mongo =True, db_name = 'MarketDataUpdate')
        
        # 2)  read out from update database and compute adjusted prices
        for ticker in os.listdir(path):
        #    print(ticker)
            #for indices, name is "SPX" ticker is "SPX Index"
            name = ticker[:ticker.find(" ")]    
            #DAX Index& IBOV Index are already total return index, so there is no need to compute adjusted prices
            if name in ["DAX","IBOV"]:
                continue
            data_old = query_part(name =name, tenor = 'spot',collection_name = collection_name,db_name = 'MarketData',adjust = 'no')
            data_new = query_part(name =name, tenor = 'spot',collection_name = collection_name,db_name = 'MarketDataUpdate', adjust = 'no')
            #we need old div data to compute new adjusted price
            data_all = data_old.append(data_new, ignore_index = True)    
            
            #data_all.loc[data_all.duplicated()]
            data_all.drop_duplicates(inplace = True)
            df = pd.DataFrame()
            for feature, temp_df in data_all.groupby("feature"):
                if not feature in ['close','open','high','low','div']:
                    continue
                print(feature)
                temp_df.rename(columns = {'value':feature}, inplace  =True)
                temp_df = temp_df[['date',feature]]
                temp_df['date'] = pd.to_datetime(temp_df['date'], unit='s')
                temp_df.set_index('date', drop=True, inplace=True)
                df = df.join(temp_df, how = 'outer')
            
            df.sort_index(inplace = True )
            df.dropna(subset = ['close','open','high','low'], how = 'all', inplace = True)    
            df.reset_index(inplace = True)
            
            for price in [ "close",'open','high','low']:
                df['adj'] = df['div'] / df[price] +1
                df['factor'] = 1.0
                date_index = (df['date'] >=  df['date'][0])
                df.loc[date_index , 'factor'] =  df.loc[date_index,'adj'].cumprod()
                df[price+"_adj"] = df[price]*df['factor']
            
            #     3)  upload adjusted price 
            data_mongo_n = pd.DataFrame()
            
            df['date'] = df['date'].apply(lambda x: int(x.value // 1e9))
            for col in ['close_adj', 'open_adj','high_adj', 'low_adj']:
        #        print(col)
                part = df[['date',col]]
                part = part.dropna(axis = 0, how = 'any')
                part.rename(columns = {col: 'value'}, inplace  =True)        
                part.insert(2, 'name',name,True)
                part.insert(2, 'ticker',name+" Index",True)    
                part.insert(2, 'tenor','spot',True)
                part.insert(2, 'adjust','all',True)
                part.insert(2, 'feature',col[:col.find("_")],True)
                data_mongo_n = data_mongo_n.append(part, ignore_index = True)    
            
            if upload:                
                data_mongo_n = data_mongo_n.loc[data_mongo_n['date'] >=data_new['date'].min()]
        #    pd.to_datetime(data_mongo_n['date'].min(), unit = 's')
            
                tomongo(data_mongo_n, collection_name = collection_name, to_mongo =True, db_name = 'MarketDataUpdate')
    

##TODO: PE_RATIO are different 
#data_old = query_part(name =name, tenor = 'spot',collection_name = 'Equity',db_name = 'MarketData')
#data_new = query_part(name =name, tenor = 'spot',collection_name = 'Equity',db_name = 'MarketDataUpdate')
#
#data_all = data_old.append(data_new, ignore_index = True)    
#same = data_all.loc[data_all.date ==1540857600]
#data_all.loc[data_all.duplicated()]


if __name__ == '__main__':
    #World equity indices
    category = "Equity"
    #update data to yesterday
    #endDate=datetime.strftime(date.today()-timedelta(1),'%Y%m%d')
    endDate = '20190816'
    #data fields for WEI
    fields = ['PX_LAST', 'PX_OPEN', 'PX_HIGH', 'PX_LOW','PE_RATIO', 'BEST_PE_RATIO',
              'PX_TO_BOOK_RATIO', 'BEST_PX_BPS_RATIO',"DVD_SH_LAST"]
    #update data for these indices
    #TODO: check which index has been updated 
    tickers = ['INDU Index',	 'SPX Index',	 'CCMP Index',	 
               'SPTSX Index',	 'SX5E Index',	 'UKX Index',	 
               'CAC Index',	'FTSEMIB Index', 'OMX Index',	 'SMI Index',	 
               'NKY Index',	'HSI Index',	 'SHSZ300 Index',	 'AS51 Index',	 
               'DAX Index','KOSPI Index',	 'MEXBOL Index',	 'IBOV Index',	 'IBEX Index']
    main(category, endDate, fields, tickers, download = True, change = False, upload = False)
