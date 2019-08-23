import pandas as pd
import numpy as np
import os
from datetime import date, datetime, timedelta
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_all, query_part



#check whether correctly downloaded
def check_downloaded(): #randomly retrive a value in bloomberg and check whether the data in MongoDB is the same
    e = query_all(collection_name = 'Equity', db_name = 'MarketData')
    arow = e[(e['ticker'] == 'SPX Index') & (e['date'] == 1261353600) & (e['adjust'] == 'all') & (e['feature'] == 'close')]
    value = arow['value'].tolist()[0]
    print(value)
    assert value == 2595.5966508246

# check whether dtype is the same as the one in MongoDB
def check_dtype():
    e = query_all(collection_name = 'Equity', db_name = 'MarketData')
    assert type(e['adjust'][0])== str
    assert e['date'][0].dtype == int
    assert type(e['feature'][0]) == str
    assert type(e['name'][0]) == str
    assert type(e['tenor'][0]) == str
    assert type(e['ticker'][0]) == str
    assert e['value'][0].dtype == float
    print('All types are correct')


#check if all the tickers have the same startDate to update from
def checkdayRange():
    e = query_all(collection_name = 'Equity', db_name = 'MarketData')
    imax = e.sort_values('date',ascending= False).groupby('ticker', as_index = False).first()
    imax['enddate'] = pd.to_datetime(imax['date'], unit= 's')
    imin = e.sort_values('date').groupby('ticker', as_index = False).first()
    imin['startdate'] = pd.to_datetime(imin['date'], unit='s')
    range = pd.merge(imax,imin, how= 'outer', on='ticker')
    range = range[['ticker', 'startdate','enddate']]
    range.to_csv('/home/pingan/PycharmProjects/test/Equity/dayRange_Equity.csv', index= False)



#check if the downloaded data and updated data have same value on the startDate
def check_same():
    e_old =  query_all(collection_name = 'Equity', db_name = 'MarketData')
    #e_new =  query_all(collection_name = 'Equity', db_name = 'MarketDataUpdate')
    e_new = pd.read_csv('/home/pingan/PycharmProjects/test/Equity/final_Equity_MarketData_update_until_20190626.csv')
    old_max = e_old.sort_values('date',ascending= False).groupby('ticker', as_index = False).first()
    old_max['old_enddate'] = pd.to_datetime(old_max['date'], unit= 's')
    new_min = e_new.sort_values('date').groupby('ticker', as_index = False).first()
    new_min['new_startdate'] = pd.to_datetime(new_min['date'], unit= 's')
    same = pd.merge(old_max, new_min, how='outer', on='ticker')
    same = same[['ticker', 'old_enddate','new_startdate']]
    #same.to_csv('/home/pingan/PycharmProjects/test/Equity/check_same_value_startDate_Equity.csv', index=False)
    print(same)



# check whether there is duplicatd data, if there is, drop duplicated data
def check_duplicated():
    e = query_all(collection_name = 'Equity', db_name = 'MarketData')
    dup = e.duplicated()
    dup = dup.values.tolist()
    count = dup.count(True)
    print(count) # if count >1 , means have duplicated value
    if count > 1:
        df_drop = e.drop_duplicates()
        #df_drop.to_csv('/home/pingan/PycharmProjects/test/Equity/no_duplicated_Equity.csv', index=False, header=False)
    print('checking Done')






