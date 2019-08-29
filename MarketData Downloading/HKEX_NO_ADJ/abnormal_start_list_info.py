import pandas as pd
import numpy as np
import os
import time

import pymongo

import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_all, query_part


list = ['8096','4335','4336','6860','667','8017', '2168', '3868', '1832', '1775', '1743', '1025']

for i in range(len(list)):
    data = query_part(name = list[i], feature = 'close', adjust = 'no', collection_name = 'HKEX_ALL', db_name ='MarketData')
    data['date'] = pd.to_datetime(data['date'], unit = 's')
    data.to_csv(os.path.join('/home/pingan/PycharmProjects/test/hkex_noadj/abnormal_start_list/' + list[i] + '.csv'), index = False)

#667 HK Equity  2006-10-05-----------2011-12-08
#1025  2003-11-20----------2016-01-06
#1743    2007-07-06------------2008-06-16
#1775     2008-04-02---------2008-11-10
#1832    2007-07-13------------2010-05-24
#2168    this ticker is belongs to another company but it died and kasai own this ticker now, so the data start from 2018-12-05
#3868    2007-10-02-----------2017-11-29
#4335    2000-05-31----------, close price duanduanxuxu
#4336       close price duanduanxuxu
#6860     2007-09-04---------------2008-03-31
#8017       2000-08-16--------2016-10-18
#8096         2002-02-26----------2017-02-03





