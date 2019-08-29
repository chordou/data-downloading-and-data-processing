import pandas as pd
import pymongo
import json
import csv
import codecs
import numpy as np
import os

# apply function in other folder
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_part




#check whether correctly downloaded
def check_downloaded(): #randomly retrive a value in bloomberg and check whether the data in MongoDB is the same
    e = query_part(name = '5', adjust = 'no', feature = 'close', collection_name = 'HKEX_ALL', db_name = 'MarketData')
    arow = e[e['date'] == 1261353600]
    value = arow['value'].tolist()[0]
    print(value)
    assert value == 86.5

# check whether dtype is the same as the one in MongoDB
def check_dtype():
    e = query_part(name = '5', adjust = 'no', feature = 'close', collection_name = 'HKEX_ALL', db_name = 'MarketData')
    assert type(e['adjust'][0])== str
    assert e['date'][0].dtype == int
    assert type(e['feature'][0]) == str
    assert type(e['name'][0]) == str
    assert type(e['category'][0]) == str
    assert e['value'][0].dtype == float
    print('All types are correct')



#check whether all ticker is downloaded, this function can also find undownloaded ticker list
def checkDownloaded():
    MarketDataUpdate = pd.read_csv(r'C:\Users\QIUCHUCHU428\PycharmProjects\test\hkex_noadj\MarketDataUpdate_list(2019.8.9).csv', dtype={'ticker': str})
    nopath = r'C:\Users\QIUCHUCHU428\PycharmProjects\test\hkex_noadj\STOCKS_NO_ADJ\20190704' # the path is where you downoload no adj price
    download_list = [name[:-14] for name in os.listdir(nopath)]

    undownload_list = []
    for ticker in MarketDataUpdate['ticker']:
        if ticker not in download_list:
            undownload_list.append(ticker)
    if len(undownload_list) == 0:
        print('all tickers downloaded')


#find max day in MarketData and min day in MarketDataUpdate
def findDayRange():
    name_list = pd.read_csv('/home/pingan/PycharmProjects/test/DataUpdate/universe1025.csv')
    total_ticker_list = [name.split()[0] for name in name_list['ticker']]

    timeDic = { }
    for name in total_ticker_list:
        df_HKEX_old_data = query_part(name = name, feature = 'close', adjust = 'no',  collection_name = 'HKEX_ALL',db_name = 'MarketData')
        df_HKEX_new = query_part(name = name, feature = 'close', adjust = 'no',  collection_name = 'HKEX_ALL_NOADJ',db_name = 'MarketDataUpdate')
        # when the data is so big that it takes much time to query so it is better not query much but query part, just query feature= close and
        # adjust = all would save much time.
        # (the solution to fix big data problem)
        if df_HKEX_old_data.empty:
            continue
        else:
            if 'date' in df_HKEX_old_data.columns:
                startDate = df_HKEX_old_data['date'].max()
                startDate = pd.to_datetime(startDate, unit='s').strftime('%Y%m%d')
                newUpdate = df_HKEX_new['date'].min()
                newUpdate = pd.to_datetime(newUpdate, unit='s').strftime('%Y%m%d')
                # print('The startDate of ' + name +  ' in HKEX_ALL is: ' + startDate)
                # print('The startDate of ' + name +  ' in HKEX_ALL is: ' + endDate)

                timeDic['startDate'] = startDate
                timeDic['newUpdate'] = newUpdate
                timeDic['name'] = name
                #time_df = pd.DataFrame(columns = ['startDate', 'endDate', 'ticker'])
                time_df = pd.DataFrame([timeDic])
                time_df.to_csv('/home/pingan/PycharmProjects/test/hkex_noadj/dayrange_match.csv', mode='a', header = False, index= False)


#checking duplicated row
def checking_duplicated(downloaded_list):
    for name in downloaded_list:
        read_path = os.path.join('/home/pingan/PycharmProjects/test/data/' + name + '.csv')
        try:
            re = pd.read_csv(read_path)

            duplicated_line = re.duplicated()
            duplicated_line = duplicated_line.values.tolist()
            print(duplicated_line.count(True)) # if count >1 , means have duplicated value
            print('checking Done')
        except pd.errors.EmptyDataError :
            continue

    return



# drop duplicated data
def dropDuplicated(filename):
    hkex_path = '/home/pingan/PycharmProjects/test/hkex/data/'
    read_path = os.path.join(hkex_path + filename + '.csv')
    af = pd.read_csv(read_path)
    if af.duplicated() ==True:
        print('contain duplicated')
    af_drop = af.drop_duplicates()
    af_drop.to_csv(hkex_path + 'duplicated.csv' , index=False, header= False)


def getfilename(path, filetype) :
    downloaded_list=[]
    for root, dirnames, filenames in os.walk(path):
         for file in filenames:
             if filetype in file:
                 downloaded_list.append(file.replace(filetype, ''))

    return downloaded_list





if __name__ == '__main__':

    # original_path = r"C:\Users\QIUCHUCHU428\PycharmProjects\test\hkex_noadj\STOCKS_NO_ADJ\20190704"
    # filetype = '.csv'
    # MarketDataUpdate_list = getfilename(original_path, filetype)
    # m = [name[:-10] for name in MarketDataUpdate_list]
    # print(m)
    # m_df = pd.DataFrame(m, columns= ['ticker'])
    # m_df.to_csv(r'C:\Users\QIUCHUCHU428\PycharmProjects\test\hkex_noadj\MarketDataUpdate_list.csv')
    #
    # # download_MongoDBdata()
    # # checking_duplicated(downloaded_list)
    findDayRange()



















































