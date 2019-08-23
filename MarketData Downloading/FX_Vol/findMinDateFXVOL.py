import os
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_part, query_all
from find_date_range import download_data_and_find_startdate
import pandas as pd




#old data day range
def dayrange():
    f = pd.read_csv('/home/pingan/PycharmProjects/test/FX_Vol/final.csv')
    #f = f[~f['date'].isin([1538092800])] # exclude 2018-09-28
    f['strdate'] = pd.to_datetime(f['date'], unit= 's')
    df = f.sort_values(by = 'strdate', ascending=False).groupby('name', as_index=False).first() #max day
    last = df[['name', 'strdate']]

    df_e = f.sort_values(by = 'strdate', ascending=True).groupby('name', as_index=False).first() #min day
    df_e  = df_e[['name', 'strdate']]

    old_date = pd.merge(last, df_e, on = 'name')


    old_date.rename(columns = {'strdate_x': 'endDate', 'strdate_y' : 'startdate' }, inplace= True)
    print(old_date)
    old_date.to_csv('/home/pingan/PycharmProjects/test/FX_Vol/dayRangefinal.csv',  index= False)


if __name__ == '__main__':
    dayrange()



