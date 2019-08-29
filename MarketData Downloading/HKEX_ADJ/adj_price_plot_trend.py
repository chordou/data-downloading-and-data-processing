import pandas as pd
import numpy as np
import os
import random
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_part,query_all
import  matplotlib.pyplot as plt
import pylab


f =pd.read_csv('/home/pingan/PycharmProjects/test/calculate_AF/358ticker_list.csv', dtype= {'ticker': str})
stock_list = f['ticker'].tolist()
stock_list = [x for x in stock_list if x != ['13', '805', '1880', '1893']]


up = stock_list[:100]
lower = stock_list[200:]
random.seed(50)
sup = random.sample(up,20)
slow = random.sample(lower, 10)

r = sup+ slow
print(r)

test_list = ['2888']

for name in test_list:
#for name in r:
    s = query_part(name= name, feature='close', adjust='all', collection_name='HKEX_ADJ', db_name='MarketDataUpdate')
    # s['strdate'] = pd.to_datetime(s['date'], unit='s')
    s['strdate'] = pd.to_datetime(s['date'], unit='s')
    s['strdate'] = s['strdate'].apply(lambda x: x.strftime('%Y-%m-%d'))
    s['rebase_mongodb'] = s['value'] / s['value'][0]


    yahoo = pd.read_csv('/home/pingan/PycharmProjects/test/calculate_AF/yahoo/' + name.zfill(4) + '.HK.csv')
    # yahoo = yahoo.drop(yahoo.index[0])
    yahoo = yahoo[yahoo['Date'] >= '2018-07-09']
    yahoo['rebase_yahoo'] = yahoo['Adj Close'] / yahoo['Adj Close'].tolist()[0]
    # print(type(s['strdate'].tolist()[0]))
    # print(type(yahoo['Date'].tolist()[0]))



    new = pd.merge(s, yahoo, how='left', left_on='strdate', right_on='Date')


    plt.cla()  # clear plot, every plot contains  one line. Otherwise, all tickers trend would plot in one picture
    plt.plot(new['Date'].tolist(), new['rebase_yahoo'].tolist(), label = 'mongodb adj close price')
    plt.plot(new['Date'].tolist(), new['rebase_mongodb'].tolist(), label = 'yahoo adj close price')
    plt.xlabel('date')
    plt.ylabel('adj close price')
    plt.title(name + ' HK Equity', fontsize='large')
    plt.legend()
    plt.savefig('/home/pingan/PycharmProjects/test/calculate_AF/adjprice_plot/' + name + '.jpg')







