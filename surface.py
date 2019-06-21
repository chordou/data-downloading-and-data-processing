import pandas as pd
import pymongo
import json
from mongodb import query_all, tomongo
import csv
import codecs
import numpy as np


x=pd.read_excel('/home/pingan/Downloads/surface.xlsx')
mul = pd.DataFrame(x)
# v = ['swap_tenor', 'option_tenor', 'date', 'value']
# mul = df[v]

# reshape everyday's data
surface_everyday = []

modified_date = mul['date'].drop_duplicates( keep='first', inplace=False)
print(modified_date)


for date in modified_date :#mul['date'] :
    data_everyday = mul.loc[date == mul['date']]
    #print(data_everyday)
    modified_swap_tenor = data_everyday['swap_tenor'].drop_duplicates(keep='first', inplace=False)
    for swap_tenor in modified_swap_tenor :#data_everyday['swap_tenor'] :
        option_tenor_column1 = data_everyday.loc[swap_tenor == data_everyday['swap_tenor']]
        #print(option_tenor_column1)
        v = list(option_tenor_column1['value'])
        surface_everyday.append(v)

        #suface_everyday.append(va)
print(surface_everyday)

list1 = list(modified_date)
list2 = list(modified_swap_tenor)
modified_option_tenor = mul['option_tenor'].drop_duplicates( keep='first', inplace=False)
list3 = list(modified_option_tenor)

df4 = pd.DataFrame(surface_everyday,
               columns = pd.MultiIndex.from_product( list1, list3 ),
               index =  list2 )