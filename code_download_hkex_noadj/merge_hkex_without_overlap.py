import pandas as pd
import numpy as np
import os
import time

#This script aims to update data without overlap in MarketData.




def getfilename(path, filetype) :
    downloaded_list=[]
    for root, dirnames, filenames in os.walk(path):
         for file in filenames:
             if filetype in file:
                 downloaded_list.append(file.replace(filetype, ""))

    return downloaded_list


dayrange = pd.read_csv('/home/pingan/PycharmProjects/test/check_Marketdata/day_range_HKEX_ALL_close_no.csv')
newdata_path = '/home/pingan/PycharmProjects/test/hkex_noadj/STOCKS_NO_ADJ/20190704/'
path = '/home/pingan/PycharmProjects/test/hkex_noadj/mergedata'
filetype = '.csv'
actual_list = dayrange['ticker'].apply(lambda x: str(x) + " HK Equity").values.tolist() #ticker name in MarketData, actually not all tickers are downloaded in universe1025.csv
total_list = getfilename(newdata_path, filetype)
downloaded_list = getfilename(path, filetype)
undownload_list = [name for name in total_list if name not in downloaded_list]


#test_list = ['3700 HK Equity']


abnormal_time_start_list = []


for name in total_list:
    content = open(os.path.join(newdata_path + name + '.csv')).read()
    length = len(content)
    if length == 1:
        print('empty')
    elif name in downloaded_list:
        continue
    elif name in actual_list: # some tickers are downloaded because it is in the universe 1025 list, but we do not need to upload them
                              # because they are not in MongoDB--MarketData


        newdata = pd.read_csv(os.path.join(newdata_path + name + '.csv'))
        n =name.replace(' HK Equity', '')
        max_date = dayrange.loc[dayrange['ticker'] == int(n)]['endDate'].values
        max_date = ','.join(str(i) for i in max_date)
        m = pd.to_datetime(max_date).strftime('%Y-%m-%d')
        if newdata['date'].min() > m:
            abnormal_time_start_list.append(name)
            # abnormal_time_start_list means: for example 3700 Equity, download data from 20180711. but the max date of it in MarketData is 20180706
            # do not know why
        else:
            newdata_no_overlap = newdata.loc[newdata['date'] > m]
            newdata_no_overlap.to_csv(os.path.join('/home/pingan/PycharmProjects/test/hkex_noadj/mergedata/' + name + '.csv'), index = False)
print(abnormal_time_start_list)


# if you want the ticker name not contain in Marketdata but in universe 1025

# vanish_ticker = []
# for name in total_list:
#     if name in actual_list:
#         continue
#     else :
#         vanish_ticker.append(name)
# vanish_ticker_df = pd.DataFrame(vanish_ticker)
# vanish_ticker_df.columns = ['vanish_ticker']
# vanish_ticker_df.to_csv('/home/pingan/PycharmProjects/test/hkex_noadj/vanish_ticker_list.csv',index=False)










