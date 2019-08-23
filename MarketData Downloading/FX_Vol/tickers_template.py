# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 10:27:46 2019

@author: ZHENGYAWEN104
"""
import pandas as pd
import numpy as np
import os
#from datetime import date, datetime, timedelta
import datetime
import copy





fx_list=['CNHJPY','EURCNH','EURHKD','EURJPY','EURUSD','HKDCNH','HKDJPY','USDCNH','USDHKD','USDJPY']
expiry_list=['ON','1W','2W','3W','1M','2M','3M',	'4M',	'6M',	'9M',	'1Y',	'18M',	'2Y',	'3Y',	'4Y',	'5Y',	'7Y',	'10Y',	'15Y']
expiry_num=[1,7,14,21,30,60,90,120,180,270,360,540,720,1080,1440,1800,2520,3600,5400]
option_type={'ATM':'V', '25 RR':'25R', '25 BF':'25B', '10 RR': '10R', '10 BF':'10B'}

option_list=list(option_type.values())

def getTicker(fx,option,expiry,suffix='BGN Curncy'):
    if fx =='HKDCNH':
        suffix='BLCS Curncy'
    ticker=fx+option+expiry+' '+suffix    
    return ticker 

tickers=pd.DataFrame(columns=['ticker','name','feature','expiry_str','tenor'])
for i in range(len(fx_list)):
    for j in range(len(expiry_list)):
        for k in range(len(option_list)):
            ticker=getTicker(fx_list[i],option_list[k],expiry_list[j],suffix='BGN Curncy')
            tickers=tickers.append({'ticker':ticker,'name':fx_list[i],
                                    'feature':list(option_type.keys())[k],
                                    'expiry_str':expiry_list[j], 'tenor':expiry_num[j]},ignore_index=True)
    
tickers.to_csv('//paicdom/paamhk/Aurelius/QiuChuchu/MarketData Downloading/FX_Vol/fx_vol_tickers.csv',index = False)