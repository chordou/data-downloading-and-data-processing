import pymongo
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_all, query_part, tomongo


import pandas as pd
import os
import  matplotlib.pyplot as plt
import pylab
import xlrd



old_factor_file = query_part(feature = 'price_af_star', collection_name = 'HKEX_AF_Daily', db_name = 'MarketDataUpdate')


adjustment_list = old_factor_file['name'].values.tolist()


# for name in ticker_list:
#    new_factor = query_part(name = name, feature = 'price_af', collection_name = 'HKEX_AF_Daily', db_name = 'MarketDataUpdate')
#    new_factor.to_csv('/home/pingan/PycharmProjects/test/calculate_AF/new_daily_factor/' + name + '.csv')
#    no_adjust = query_part(name = name, feature = {'$in':['close', 'open', 'high', 'low']}, collection_name = 'HKEX_ALL_NOADJ', db_name = 'MarketDataUpdate')
#    no_adjust.to_csv('/home/pingan/PycharmProjects/test/calculate_AF/no_adjust_price/' + name + '.csv')

# calculate ticker who has adjustmentfactor

adjustment_list = [x for x in adjustment_list]
print(adjustment_list)
test_list = ['1136']


for name in adjustment_list:
#for name in test_list:
    try :
        print(name)
        fac = query_part(name = name, feature = 'price_af', collection_name = 'HKEX_AF_Daily', db_name = 'MarketDataUpdate')
        no_ad = query_part(name = name, adjust = 'no', feature = {'$in':['close', 'open', 'high', 'low']},
                           collection_name = 'HKEX_ALL_NOADJ', db_name = 'MarketDataUpdate' )

        if fac.empty or no_ad.empty:
            continue

        elif fac['value'].empty or no_ad['value'].empty:
            continue
        else:
            c = pd.merge(fac, no_ad, on='date', how='right')
            #c['part'] = c['value_y'] / c['value_x']
            old_factor = old_factor_file[old_factor_file['name'] == name]['value'].tolist()[0]

            # c['adj_price'] = c['part'].apply(lambda x: x * old_factor)
            c['adj_price'] = c['value_y'] * c['value_x'] / old_factor
            # print(c)
            fina = c[['adj_price', 'name_x', 'date', 'feature_y', 'category_y']]

            fina = fina.rename(
                columns={'name_x': 'name', 'category_y': 'category', 'feature_y': 'feature', 'adj_price': 'value'})
            fina.insert(len(fina.columns), 'adjust', 'all', True)
            tomongo(fina, collection_name = 'HKEX_ADJ', to_mongo=True,replace=False,db_name='MarketDataUpdate')
            # fina.to_csv('/home/pingan/PycharmProjects/test/calculate_AF/cal_adj_price/' + name + '.csv',
            #             index=False)
            #print(name)

    except pd.io.common.EmptyDataError :
        print('empty')




# calculate ticker without adjustmentfactor


total = pd.read_csv('/home/pingan/PycharmProjects/test/MarketData Downloading/universe1025(updated4stocks).csv', dtype= {'ticker' : str})
unadjustment_list = [ticker for ticker in total['ticker'] if ticker not in adjustment_list]
print(unadjustment_list)


for name in unadjustment_list:
    try:
        print(name)
        c =  query_part(name = name, adjust = 'no', feature = {'$in':['close', 'open', 'high', 'low']},
                           collection_name = 'HKEX_ALL_NOADJ', db_name = 'MarketDataUpdate' )
        if c.empty:
            continue

        elif c['value'].empty:
            continue
        else:
            c = c.drop(columns= 'adjust')
            c.insert(len(c.columns), 'adjust', 'all', True)
        tomongo(c, collection_name='HKEX_ADJ', to_mongo=True, replace=False, db_name='MarketDataUpdate')
        #c.to_csv('/home/pingan/PycharmProjects/test/calculate_AF/adj_price_without_adjustment_factor/' + name + '.csv', index = False)

    except pd.errors.EmptyDataError:
        print('empty')






























