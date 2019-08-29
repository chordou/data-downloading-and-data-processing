import pandas as pd
import numpy as np
import os
import datetime
import copy
import time
import sys
sys.path.append('//paicdom/paamhk/Aurelius/QiuChuchu/DataUpdate/tools')
from mongodb import tomongo


a = pd.read_csv('//paicdom/paamhk/Aurelius/Data/DataDownloading/DataUpdate/data/HKEX_AF/20190822/AF.csv')
a = a[~a['ticker'].isin(['ticker'])] # data cleaning
tomongo(a, collection_name='HKEX_AF', db_name='MarketDataUpdate', to_mongo=True,replace=False)



