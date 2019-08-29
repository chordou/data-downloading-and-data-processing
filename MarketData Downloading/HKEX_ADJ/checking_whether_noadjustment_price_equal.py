import pandas as pd
import numpy as np
import os
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_all, query_part


def getfilename(path, filetype) :
    downloaded_list=[]
    for root, dirnames, filenames in os.walk(path):
         for file in filenames:
             if filetype in file:
                 downloaded_list.append(file.replace(filetype, ''))

    return downloaded_list


path = '/home/pingan/PycharmProjects/test/calculate_AF/adj_price_without_adjustment_factor'
without = getfilename(path, '.csv')
print(without)


test_list = ['1451']
equal_count = []
not_equal_ticker = []



#notequal_df = pd.DataFrame()

#for name in test_list:
for name in without:
    check_column = []
    adj = query_part(name = name, feature = {'$in' : ['close', 'high', 'open', 'low'] }, adjust = 'all', collection_name = 'HKEX_ALL', db_name = 'MarketData')
    noa = query_part(name = name, feature = {'$in' : ['close', 'high', 'open', 'low'] }, adjust = 'no', collection_name = 'HKEX_ALL', db_name = 'MarketData')
    if adj.empty or noa.empty:
        continue

    else:
        new = pd.merge(noa, adj, on=['date', 'feature', 'name', 'category'], how= 'outer')
        for i in range(len(new['date'])) :
           if new['value_x'][i] == new['value_y'][i]:
               x = True
               check_column.append(x)
           elif np.isnan(new['value_x'][i])  & np.isnan(new['value_y'][i]) :
               x = True
               check_column.append(x)
           else:
               x = False
               print(name)
               not_equal_ticker.append(name)
               check_column.append(x)

    c = check_column.count(False)
    #print(c)
    equal_count.append(c)

l = len(equal_count)
zero = equal_count.count(0)
if l == zero :
    print('data without adjustment factor: adj price == no adjust price')
else:
    not_equal_ticker = set(not_equal_ticker)
    print('somthing wrong')
    print(not_equal_ticker)




