# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 09:55:38 2019

@author: ZHENGYAWEN104
"""

import pandas as pd
import numpy as np
import os
#from datetime import date, datetime, timedelta
import datetime
from BBG import BloombergFormula as bbg
from DataUpdate.tools.mongodb import query_all, query_part,tomongo
import copy
import sys
sys.path.append('/home/pingan/PycharmProjects/test/downloadCode')
from uploadCsvToMongDB import UploadCsvFileToMongo



# chuchu: there are 2236 tickers in the list, we'd better first download data then reshape data and upload data
# part 1: set download = True. change = False, upload = False
# part 2: download = False. change = True, upload = False
# part 3: download = False. change = False, upload = True
#after every step, check the correctness of data

def main(category, endDate, fields,fields_new, tickers, collection_name,startDate, download = False,  change = False,upload = False):
    #set the path to store downloaded data by update date
    update_date = datetime.date.today().strftime('%Y%m%d')

    #path = os.path.join("//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/"+category+"/"+update_date)
    # chuchu :personalize folder is good, but if updated data is big, it is better to fix the folder
    path = os.path.join('W:\\Data\\DataDownloading\\DataUpdate\\data\\STOCKS_NO_ADJ\\20190704')
    if not os.path.isdir(path):
        os.makedirs(path)
        
     # read data out from MongoDB to get tickers and startDate
    assert len(tickers) == 1
    ticker = tickers[0]
    name = ticker[:ticker.find(" ")]
    if startDate is None:        
        data = query_part(db_name = 'MarketDataUpdate',collection_name = 'HKEX_ALL_NOADJ', name = name, adjust= 'no', feature = 'close')
        #since HKEX_ALL_NOADJ is extremely large, we should query_part in order to save time
        
        #download data from startDate, so updated data has one day duplicated
        #Lucas: solved duplicated date by using timedelta +1day
        startDate = data['date'].max()
        startDate = pd.to_datetime(startDate, unit='s')+pd.Timedelta('1 days')
        startDate = startDate.strftime('%Y%m%d')
        #chuchu: better not to fix the startdate, because sometime the end date of different ticker in MarketData is different.
        # otherwise it takes much time to datacleaning


    if download:
        #download data from bloomberg and store each ticker's data as a csv file 
        # for no adjusted data, use adjust_sigle_ticker_BDH

        dataFFF,invalidSec=bbg.BBGFormula(ticker).adjust_sigle_ticker_BDH(fields,startDate,endDate,split=False, adjustNormal=False,adjustAbnormal=False)
        download_path = path+ '\\'+ ticker+'.csv'
        dataFFF.to_csv(download_path,index=False)



    if change:
        # change the format
        name_list = getfilename(path, filetype)

        old = copy.deepcopy(fields)
        new = copy.deepcopy(fields_new)

        for file in name_list:
            # check whether new data is empty.
            # This is because some enquities has already died, no more new data.
            try:
                data_mongo = pd.DataFrame()
                dataone = pd.read_csv(os.path.join(path + file + '.csv'))

            except pd.io.common.EmptyDataError:
                print('no new data for' + str(file))
                continue

            if len(list(dataone.columns)) == 0:
                continue
            else:
                dataone.rename(columns=dict(zip(old, new)), inplace=True)

                dataone['date'] = dataone['date'].apply(lambda x: int(pd.to_datetime(x, yearfirst=True).value // 1e9))
                for col in list(set(dataone.columns) - set(['date', 'ticker'])):
                    part = dataone[['date', col]]
                    part = part.dropna(axis=0, how='any')
                    part.rename(columns={col: 'value'}, inplace=True)
                    part.insert(len(part.columns), 'name', file[:file.find(" ")], True)
                    part.insert(len(part.columns), 'tenor', 'spot', True)
                    part.insert(len(part.columns), 'adjust', 'no', True)
                    part.insert(len(part.columns), 'feature', col, True)
                    data_mongo = data_mongo.append(part, ignore_index=True)
                    # print(data_mongo)
            filepath =os.path.join(path + '/' + file + '.csv')
            data_mongo.to_csv(filepath, index=False)
            # chuchu: better to download the final format in the computer, since we should double check the date and format



    #1) upload downloaded csv file of no adjusted price&dividend
    if upload:
        #chuchu : check whether the file is empty, MongoDB is not allowed to upload enpty file
        #it would raise error, because of "db_cm.insert_many"
        content = open(path).read()
        length = len(content)
        if length == 1:
            print('no new date')
        else:
            UploadCsvFileToMongo(filepath, collection_name, to_mongo=False, replace=False, db_name='MarketDataUpdate')
        # chuchu: I think when downloading data, all data should be download first, then upload in case of uploading duplicated data
        # mongodb would not check duplicated data
        
def getfilename(path, filetype) :
    downloaded_list=[]
    for root, dirnames, filenames in os.walk(path):
         for file in filenames:
             if filetype in file:
                 downloaded_list.append(file.replace(filetype, ''))

    return downloaded_list



if __name__ == '__main__':
    import time
    category = 'STOCKS_NO_ADJ'
    #This is a choice to update until yesterday
    #endDate = datetime.strftime(date.today()-timedelta(1),'%Y%m%d')
    endDate = '20190626'
    collection_name = 'HKEX_ALL_NOADJ'
    fields = ["PX_LAST","PX_OPEN","PX_HIGH","PX_LOW","PX_VOLUME","TURNOVER", 
              "SHORT_SELL_NUM_SHARES"	,"SHORT_SELL_TURNOVER","CUR_MKT_CAP"]
    #fields' name in MongoDB
    fields_new =  ['close', 'open','high', 'low','volume', 'turnover', 'volume_short_sale',  'turnover_short_sale', 'mkt_cap']
    #Download no adjusted prices for only one ticker
    #tickers = ["700 HK Equity"]
    #main(category, endDate, fields,fields_new, tickers, collection_name,startDate = "20180706", download = False, upload = False)
    #download data for the tickers in this excel

    downloaded_path = 'W:\\Data\\DataDownloading\\DataUpdate\\data\\STOCKS_NO_ADJ\\20190704'
    filetype = '.csv'
    downloaded_list = getfilename(downloaded_path, filetype)
    # if there is something wrong with network, redownload would not download the same ticker


    universe = pd.read_csv('/home/pingan/PycharmProjects/test/download_adjustmentFactor/universe1025.csv')
    # some ticker would not download finally, so it may raise error when querying an empty dataframe

    tickername = universe['ticker']
    tickername = tickername.values.tolist()
    for name1 in tickername:
        if name1 in downloaded_list:
            continue
        elif bbg.NotFoundError:
            # because sometimes if we download data from BBG too frequently, there may encounter connection errors
            # so we need to waite for a few seconds
            time.sleep(120)
        else :
            tickers = [universe.ticker[i]]
            # tickers = ['2 HK Equity']
            main(category, endDate, fields, fields_new, tickers, collection_name, startDate= None, download=True,
                 upload=True)

    print('HKEX_ALL_NOADJ updated!')





        
        
        
        
        
        
        
        
        
        
        
        
        

