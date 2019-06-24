import pandas as pd
import pymongo
import json
from mongodb import query_all, tomongo
import csv
import codecs
import numpy as np


x=pd.read_csv('/home/pingan/PycharmProjects/test/data_ir_vol.csv')
mul = pd.DataFrame(x)
mul = mul[['feature', 'name', 'value', 'swap_tenor', 'option_tenor'] ]
print(mul)

modified_name = mul['name'].drop_duplicates(keep='first', inplace=False)
modified_swap_tenor = mul['swap_tenor'].drop_duplicates(keep='first', inplace=False)
modified_date = mul['date'].drop_duplicates(keep='first', inplace=False)

same_date_df = pd.DataFrame()
same_name_df = pd.DataFrame()
same_swap_tenor_df = pd.DataFrame()

for name in modified_name :
    same_name = mul.loc[name == mul['name']]
    same_name = 
    same_name_df = pd.concat([same_name_df,same_name], axis= 1)
    for swap_tenor in modified_swap_tenor :
        same_swap_tenor = same_name_df.loc[swap_tenor  == mul['swap_tenor']]
        same_swap_tenor_df = pd.concat([same_swap_tenor_df,same_swap_tenor], axis= 1)
print(same_swap_tenor_df)


