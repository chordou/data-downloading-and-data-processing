import pymongo
ip = '10.63.16.76'

#Attention:
# make sure collection_name and db_name is what you need
db_name = 'MarketDataUpdate'
collection_name = 'HKEX_ALL_NOADJ'

mng_client = pymongo.MongoClient(ip, 27017)
mng_db = mng_client[db_name]

db_cm = mng_db[collection_name]
#myquery = { 'date' : {'$gte': 1531065600}, 'adjust': 'no'}
myquery = { }
db_cm.delete_many(myquery)

