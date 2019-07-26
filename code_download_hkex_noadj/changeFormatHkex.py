import pandas as pd
import numpy as np
import os
import time
import copy


def getfilename(path, filetype) :
    list=[]
    for root, dirnames, filenames in os.walk(path):
         for file in filenames:
             if filetype in file:
                 list.append(file.replace(filetype, ''))

    return list

def changeFormatHkex():
    path = '/home/pingan/PycharmProjects/test/hkex_noadj/mergedata/'
    filetype = '.csv'

    fields = ["PX_LAST","PX_OPEN","PX_HIGH","PX_LOW","PX_VOLUME","TURNOVER",
                  "SHORT_SELL_NUM_SHARES"	,"SHORT_SELL_TURNOVER","CUR_MKT_CAP"]
    fields_new =  ['close', 'open','high', 'low','volume', 'turnover', 'volume_short_sale',  'turnover_short_sale', 'mkt_cap']


    old = copy.deepcopy(fields)
    new = copy.deepcopy(fields_new)


    name_list = getfilename(path, filetype)


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
                part.insert(len(part.columns), 'adjust', 'no', True)
                part.insert(len(part.columns), 'feature', col, True)
                part.insert(len(part.columns), 'category', 'Equity', True)
                data_mongo = data_mongo.append(part, ignore_index=True)
            #print(file, 'Done')
        data_mongo.to_csv(os.path.join('/home/pingan/PycharmProjects/test/hkex_noadj/changeformatdata/' + file + '.csv'), index= False)

