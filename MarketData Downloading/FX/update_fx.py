# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 09:14:47 2019

@author: LUCHENWEI861
"""

#Part1
#import win32com, use this package to trigger macro and save data in the excel

#REFERENCE FILE PATH AND RUN MACROS#
import os
import win32com.client
filename_macro = r'Z:\Bohui\Factset\macro_for_refresh_1029.xlsm'    
if os.path.exists(filename_macro):
    xl = win32com.client.Dispatch('Excel.Application')
    xl.Workbooks.Open(Filename = filename_macro, ReadOnly=1)
    xl.Application.Run("UpdateTickerAndRefresh_fx")
    xl.Application.Quit()
    del xl
#PRINT FINAL COMPLETED MESSAGE#
print("Macro refresh completed!")

#Lucas-19.5.23:Seems that Bohui's macro requires for something in her path,
#which can't reach. so this macro doesn't work now, please further check that.

#Part2
#use uploadAndQuery_FX.py to upload new data to mongodb
import pandas as pd
import numpy as np
import pymongo
import json
import time
import calendar
from datetime import datetime
import os
#import pprint as pp 
ip = '10.63.16.76'

def parse_time(time_string,time_format='%d/%m/%Y'):
    return calendar.timegm(datetime.strptime(time_string, time_format).timetuple())
def inverse_parse_time(time_number, unit='s'):
    return pd.to_datetime(time_number,unit='s')

def tomongo(data_mongo,collection_name,to_mongo=False,replace=False,db_name='Market_data'):
    
    data_json = json.loads(data_mongo.to_json(orient='records'))
    
    if to_mongo:
        mng_client = pymongo.MongoClient( ip ,27017)
        mng_db = mng_client[db_name]
        db_cm = mng_db[collection_name]
        if replace:
            db_cm.delete_many({})        
        db_cm.insert_many(data_json)
        mng_client.close()
    return data_json


#-------------upload the FX spot and forward rate
path=r"Z:\Bohui\Factset"
#files=os.listdir(path)

expiry = pd.DataFrame(columns = ['expiry_str','expiry_num'])

expiry['expiry_str'] = ['SPOT', 'ON', 'TN', 'SW', '1M', '2M', '3M', '6M', '9M', '1Y', '2Y','5Y']
expiry['expiry_num'] = [0, 1,2,7,30,60,90,180, 270,360,720,1800]


data_mongo=pd.DataFrame()
part = pd.DataFrame()
fx_name_list=['CNHJPY','EURCNH','EURHKD','EURJPY','EURUSD','HKDCNH','HKDJPY','USDCNH','USDHKD','USDJPY']

for file in fx_name_list:

    print(file)
    file = 'fx'+file+'.csv'
    #import data downloaded from Bloomberg
    #Lucas:I think this should be "From Factset", check the macro.
    diry=os.path.join(path,file)
    data=pd.read_csv(diry)
    
    data = data.dropna(axis = 1, how='all')
    
    data.insert(0,'ticker',file[:-4],True)
    
    #data = data.replace('empty',np.nan)   
    
    data.columns
    
    data.rename(columns = {'SPOT_BID' : 'SPOT_B', 'SPOT_ASK':'SPOT_A'}, inplace= True)
    for i in range(3,26,2):
        
        
        col_b=data.columns[i-1]
        col_a=data.columns[i]
        
        print(col_b, col_a)
        part = data[['ticker','date',col_a, col_b]]
        
        part.rename(columns = {col_a :col_a[col_a.find('_')+1:] ,col_b :col_b[col_b.find('_')+1:]}, inplace = True)
        
        part.insert(0,'expiry_str',col_a[:col_a.find('_')],True )
        
        data_mongo=data_mongo.append(part)
        part = pd.DataFrame()
    
    
    data_mongo = data_mongo.dropna(axis = 0, subset= ['A','B'])
    #data_mongo[['A','B']]=data_mongo[['A','B']].astype('float64')
    data_mongo.rename(columns={'A':'ask', 'B':'bid'},inplace =True)
    data_mongo['date']= data_mongo['date'].apply(lambda x: parse_time(x, "%d/%m/%Y"))    
    data_mongo=data_mongo.reset_index(drop = True)
    
    data_mongo = pd.merge(data_mongo , expiry , on = 'expiry_str')    
    data_mongo = data_mongo[['ticker', 'date', 'ask', 'bid', 'expiry_num','expiry_str']]
    
    data_json = tomongo(data_mongo,collection_name = 'FX',to_mongo=True,replace=False,db_name='MarketDataUpdate')
    data_mongo=pd.DataFrame()