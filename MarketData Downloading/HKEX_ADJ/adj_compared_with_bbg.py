import pandas as pd
import pymongo
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_all, query_part
from mongodb import query_part,query_all
import  matplotlib.pyplot as plt

import pandas as pd
import os
import  matplotlib.pyplot as plt
import numpy as np
import time

# #358list, no new data
# n = pd.read_csv('/home/pingan/PycharmProjects/test/hkex_noadj/no_new_data_in_MarketDataUpdate.csv', dtype= {'ticker': str})
# s = pd.read_csv('/home/pingan/PycharmProjects/test/calculate_AF/358ticker_list.csv', dtype= {'ticker': str})
#
# abnormal358 = [name for name in n['ticker'].tolist() if name in s['ticker'].tolist()]
# print(abnormal358)

n= '/home/pingan/PycharmProjects/test/adjprice/STOCKS_ADJ/20190813'
stock358 = pd.read_csv('/home/pingan/PycharmProjects/test/calculate_AF/358ticker_list.csv',dtype={'ticker': str})


check_list = [name.split()[0] for name in os.listdir(n) if name.split()[0] in stock358['ticker'].tolist()]
print(check_list)

test_list= ['4']
#for name in test_list:
for name in check_list:
    try:
        s = query_part(name=name, feature='close', adjust='all', collection_name='HKEX_ADJ', db_name='MarketDataUpdate')
        # s['strdate'] = pd.to_datetime(s['date'], unit='s')
        s['strdate'] = s['date'].apply(lambda x: pd.to_datetime(x, unit='s').strftime('%Y-%m-%d'))
        s = s.loc[s['strdate'] >= '2018-07-09']

        s['rebase_mongodb'] = s['value'] / s['value'].tolist()[0]





        bbg = pd.read_csv('/home/pingan/PycharmProjects/test/adjprice/changeFormat/' + name + '.csv')
        bbg['strdate'] = bbg['date'].apply(lambda x: pd.to_datetime(x, unit='s').strftime('%Y-%m-%d'))
        #bbg = bbg[bbg['strdate'] >= '2018-01-02']
        bbg = bbg[(bbg['adjust'] == 'all') & (bbg['feature'] == 'close')]


        bbg['rebase_bbg'] = bbg['value'] / bbg['value'].tolist()[0]
        # print(type(s['strdate'].tolist()[0]))
        # print(type(bbg['strdate'].tolist()[0]))

        new = pd.merge(s, bbg, how='left', on='strdate')
        # no = new[new.isnull().values==True]['strdate'].tolist()[-1] #find the last date of missing value
        # newdate =  pd.to_datetime(no)+pd.Timedelta('1 days')

        plt.cla()  # clear plot, every plot contains  one line. Otherwise, all tickers trend would plot in one picture
        plt.plot(new['strdate'].tolist(), new['rebase_bbg'].tolist(), label='mongodb adj close price')
        plt.plot(new['strdate'].tolist(), new['rebase_mongodb'].tolist(), label='bbg adj close price')
        plt.xlabel('date')
        plt.ylabel('adj close price')
        plt.title(name + ' HK Equity', fontsize='large')
        plt.legend()
        #plt.show()
        plt.savefig('/home/pingan/PycharmProjects/test/adjprice/adj_plot_compared_with_bbg/' + name + '.jpg')


        #print(no)


    except pd.io.common.EmptyDataError:
        continue