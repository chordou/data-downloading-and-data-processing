import os
import pandas as pd




def no_new_data_list():
    file_path ='/home/pingan/PycharmProjects/test/hkex_noadj/STOCKS_NO_ADJ/20190704/' #downloaded data

    no_new_data_list = []
    for name in os.listdir(file_path):
         content = open(os.path.join(file_path, name)).read()
         length = len(content)
         if length == 1:
             print('empty')
             no_new_data_list.append(name)
    df_no = pd.DataFrame(no_new_data_list)

    df_no.columns = ['ticker']
    df_no['ticker'] = df_no['ticker'].apply(lambda x: x.replace(' HK Equity.csv', ''))
    print(df_no)
    df_no.to_csv('/home/pingan/PycharmProjects/test/hkex_noadj/no_new_data_in_MarketDataUpdate.csv')
    return

