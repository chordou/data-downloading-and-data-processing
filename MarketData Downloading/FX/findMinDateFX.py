import os
import sys
sys.path.append('/home/pingan/PycharmProjects/test/DataUpdate/tools')
from mongodb import query_part, query_all
from find_date_range import download_data_and_find_startdate
import pandas as pd


def concatManyFile():
list=[]
path = '/home/pingan/PycharmProjects/test/FX/dayRange'
for file in os.listdir(path):
    df = pd.read_csv(os.path.join(path, file))
    list.append(df)

new = pd.concat(list)
print(new)
new.to_csv('/home/pingan/PycharmProjects/test/FX/dayRange/new.csv')




#old data day range
def dayrange(filename):

    f = pd.read_csv('/home/pingan/PycharmProjects/test/FX/' + filename + '.csv')
    #f = f[~f['date'].isin([1538092800])] # exclude 2018-09-28
    f['strdate'] = pd.to_datetime(f['date'], unit= 's')
    df = f.sort_values(by = 'strdate', ascending=False).groupby('ticker', as_index=False).first()
    last = df[['ticker', 'strdate']]

    df_e = f.sort_values(by = 'strdate', ascending=True).groupby('ticker', as_index=False).first()

    df_e  = df_e[['ticker', 'strdate']]
    old_date = pd.merge(last, df_e, on = 'ticker')
    print(old_date)

    old_date.rename(columns = {'strdate_x': 'endDate', 'strdate_y' : 'startdate' }, inplace= True)
    old_date.to_csv('/home/pingan/PycharmProjects/test/FX_Vol/dayRange' + filename + '.csv',  index= False)


if __name__ == '__main__':
    dayrange('merge_fx_vol')



