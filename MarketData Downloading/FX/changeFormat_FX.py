import pandas as pd
import numpy as np
import pymongo
import json
import time
import calendar
from datetime import datetime
import os
#import pprint as pp 

def parse_time(time_string,time_format='%d/%m/%Y'):
    return calendar.timegm(datetime.strptime(time_string, time_format).timetuple())
def inverse_parse_time(time_number, unit='s'):
    return pd.to_datetime(time_number,unit='s')




#-------------upload the FX spot and forward rate
path="C:\\Users\\QIUCHUCHU428\\PycharmProjects\\test\\FX\\fx\\"
#files=os.listdir(path)

expiry = pd.DataFrame(columns = ['expiry_str','tenor'])

expiry['expiry_str'] = ['SPOT', 'ON', 'TN', 'SW', '1M', '2M', '3M', '6M', '9M', '1Y', '2Y','5Y']
expiry['tenor'] = [0, 1,2,7,30,60,90,180, 270,360,720,1800]


data_mongo=pd.DataFrame()
part = pd.DataFrame()
fx_name_list=['CNHJPY','EURCNH','EURHKD','EURJPY','EURUSD','HKDCNH','HKDJPY','USDCNH','USDHKD','USDJPY']




for file in fx_name_list:

    print(file)

    #import data downloaded from factset
    diry=os.path.join(path + file + '.csv')
    data=pd.read_csv(diry)
    
    data = data.dropna(axis = 1, how='all')

    data.insert(0,'name',file,True)
    
    #data = data.replace('empty',np.nan)   
    
    data.columns
    
    data.rename(columns = {'SPOT_BID' : 'SPOT_B', 'SPOT_ASK':'SPOT_A'}, inplace= True)
    for i in range(3,26,2):
        
        
        col_b=data.columns[i-1]
        col_a=data.columns[i]
        
        print(col_b, col_a)
        part = data[['name','date',col_a, col_b]]
        
        part.rename(columns = {col_a :col_a[col_a.find('_')+1:] ,col_b :col_b[col_b.find('_')+1:]}, inplace = True)
        
        part.insert(0,'expiry_str',col_a[:col_a.find('_')],True )
        part.insert(len(part.columns), 'feature','rate',True)
        
        data_mongo=data_mongo.append(part)
        part = pd.DataFrame()
    
    
    data_mongo = data_mongo.dropna(axis = 0, subset= ['A','B'])
    #data_mongo[['A','B']]=data_mongo[['A','B']].astype('float64')
    data_mongo.rename(columns={'A':'ask', 'B':'bid'},inplace =True)
    data_mongo['date']= data_mongo['date'].apply(lambda x: parse_time(x, "%d/%m/%Y"))
    data_mongo['value'] = (data_mongo['bid'] + data_mongo['ask']) /2



    data_mongo=data_mongo.reset_index(drop = True)
    
    data_mongo = pd.merge(data_mongo , expiry , on = 'expiry_str')    
    data_mongo = data_mongo[['name', 'date', 'ask', 'bid', 'tenor','expiry_str','value','feature']]
    data_mongo = data_mongo[data_mongo['date'] > 1538092800 ]
    #data_json = tomongo(data_mongo,collection_name = 'FX',to_mongo=True,replace=False,db_name='Market_data')
    data_mongo.to_csv('C:\\Users\\QIUCHUCHU428\\PycharmProjects\\test\\FX\\data_mongo\\' +  file + '.csv', index = False)
    data_mongo = pd.DataFrame()



    

