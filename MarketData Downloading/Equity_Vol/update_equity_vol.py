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
import blpapi

def BDH_override(ticker, fields, override_1, value1, override_2, value2, startDate,endDate):
    '''
    Just get one ticker's data, fields can only be one in this case
    
    Return dataframe, the columns are "dates","ticker","fields[0]"...
    The dates only include active days
    The calendar is local 
    For each column, the data is the same as:
        =BDH(ticker, fields, startDate, endDate,  override_1,value1,   override_2,value2)
     
    the format of date should be "%Y%mm%dd" like "20181231"
    
    '''
    # Session
    session = blpapi.Session()   
    if not session.start():    
        raise NotFoundError('fail to start session')
        print('fail to start session')
                
    # Service
    session.openService("//blp/refdata")
    if not session.openService('//blp/refdata'):
        raise NotFoundError('fail to open service')
        print('fail to open service')  
    
        
    service = session.getService("//blp/refdata")
  
    # Create Request
    request = service.createRequest("HistoricalDataRequest")

    request.getElement("securities").appendValue(ticker)
    for field in fields:
        request.getElement("fields").appendValue(field)
    request.set("startDate",startDate)
    request.set("endDate",endDate)
    
    request.set("periodicitySelection", "DAILY")
    #Returns blank for the "value" value within the data element for this field.
    request.set("nonTradingDayFillMethod","NIL_VALUE")
    #Include only active days (days where the instrument and field pair updated) in the data set returned
    request.set("nonTradingDayFillOption", "ACTIVE_DAYS_ONLY")
    
    overrides = request.getElement("overrides")
    override1 = overrides.appendElement()
#    override1.setElement("fieldId", "IVOL_MATURITY")
#    override1.setElement("value", "MATURITY_30D")
#    override2 = overrides.appendElement()
#    override2.setElement("fieldId", "IVOL_MONEYNESS_LEVEL")
#    override2.setElement("value", "MONEY_LVL_80_0")
    
    override1.setElement("fieldId", override_1)
    override1.setElement("value", value1)
    override2 = overrides.appendElement()
    override2.setElement("fieldId", override_2)
    override2.setElement("value", value2)

    session.sendRequest(request)
        
    endReached = False    
    count=0
    fdata=pd.DataFrame()    
    invalidSec={}
    dates= []
    lists=[[] for _ in range(len(fields))]
    while not endReached:
    
        blpevent = session.nextEvent()
        if blpevent.eventType() == blpapi.Event.PARTIAL_RESPONSE or blpevent.eventType() == blpapi.Event.RESPONSE:
    
            for msg in blpevent:  
                #print(msg)  
                if msg.hasElement("responseError"):
                    error=msg.getElement("responseError").getElement("subcategory").getValue()
                    invalidSec.update({ticker: error})
                    print(ticker, error)
                else:
                    fieldData = msg.getElement("securityData").getElement("fieldData") 
                    sec = msg.getElement("securityData").getElement("security").getValue()
                                                    
                    if fieldData.numValues()==0:                    
                        if msg.getElement("securityData").hasElement("securityError"):
                            error=msg.getElement("securityData").getElement("securityError").getElement("subcategory").getValue()
                            invalidSec.update({sec:error})
                        else:
                            invalidSec.update({sec: endDate +" is ealier than the start date of "+ sec})
                    else: 
                        dataframe = pd.DataFrame()
                        for i in range(0, fieldData.numValues()):                                             
                            dates.append(str(fieldData.getValue(i).getElement("date").getValue()))            
                            for j in range(len(fields)):
                                if fieldData.getValue(i).hasElement(fields[j]):
                                    lists[j].append(fieldData.getValue(i).getElement(fields[j]).getValue()) 
                                else:
                                    lists[j].append(np.nan)                                                                    
                        dataframe['date']=dates
                        dataframe['ticker']=sec
                        for j in range(len(fields)):
                            dataframe[fields[j]]=lists[j]
                    
                        fdata=fdata.append(dataframe)
                        dates= []
                        lists=[[] for _ in range(len(fields))]
                    
                        count+=1
                        print(sec)
                        
        if blpevent.eventType() == blpapi.Event.RESPONSE:
            endReached = True
                
    return fdata,invalidSec

def main(category, endDate,ticker, fields,tenor_list,moneyness_list, collection_name, download = False, upload = False):
    #set the path to store downloaded data by update date
    update_date = date.today().strftime('%Y%m%d')
    #path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    if not os.path.isdir(path):
        os.makedirs(path)
    # read data out from MongoDB to get tickers and startDate
    #data = query_all(db_name = 'MarketDataUpdate',collection_name = collection_name)
    
    #TODO: check if all the tickers have the same startDate to update from 
    #download data from startDate, so updated data has one day duplicated
    #Lucas: solved duplicated date by using timedelta +1day
#    startDate = data['date'].max()
#    startDate = pd.to_datetime(startDate, unit='s')+pd.Timedelta('1 days')
#    startDate = startDate.strftime('%Y%m%d')
    startDate = '20190627'
    
    #Lucas:change startdate to one week before today
    #startDate = datetime.strftime(date.today()-timedelta(8),'%Y%m%d')

    if download:
        data_mongo = pd.DataFrame()
        for terms in tenor_list[:]:
            for moneyness in moneyness_list[:]:
                print(terms) 
                print(moneyness)
                value1 = 'MATURITY_'+ terms
                value2= "MONEY_LVL_"+moneyness+"_0"
                data, inv = BDH_override(ticker, fields, override_1, value1, override_2, value2, startDate,endDate)
                if not data.empty:
                    data.rename(columns = {fields[0]: 'value'}, inplace = True)
                    data['tenor'] = terms
                    data['feature'] =moneyness
                    data_mongo = data_mongo.append(data)
        
        data_mongo['date'] = data_mongo['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst = True).value // 1e9)) 
        data_mongo['tenor']=data_mongo['tenor'].apply(lambda x: int(x[ :x.find('D')]))
#        data_mongo = data_mongo.replace('NKY Index', 'NKY')
        data_mongo['ticker'] = data_mongo['ticker'].apply(lambda x: x[:x.find(" ")])
        data_mongo.rename(columns = {'ticker':'name'}, inplace = True)
        data_mongo.to_csv(path + '/' + ticker + collection_name + '.csv',index=False)    
    if upload:
        tomongo(data_mongo,collection_name = 'Equity_Vol',to_mongo=True,replace=False,db_name='MarketDataUpdate')


if __name__ == '__main__':
    category = 'Equity_Vol'
    #Lucas:change endDate to yesterday. if we run this file every monday, it'll update the data of last week.
    #endDate = datetime.strftime(date.today()-timedelta(1),'%Y%m%d')
    endDate = '20190816'
    collection_name = 'Equity_Vol'

    fields = ['IVOL_MONEYNESS']
    override_1 ="IVOL_MATURITY"
    override_2 = "IVOL_MONEYNESS_LEVEL"
    
    
    tenor_list = ['30D','60D',	'90D',	'180D','360D','540D','720D']
    moneyness = [80 ,85	,90,	95	,97.5	,100,102.5	,105,	110,	115	,120]
    moneyness_list = [str(x) for x in moneyness]
    for ticker in ['HSI Index', 'NKY Index', 'SPX Index', 'HSI Index', 'NKY Index']:
        main(category, endDate,ticker, fields,tenor_list,moneyness_list, collection_name, download = True, upload = False)
