import pymongo
ip = '10.63.16.76'

#Attention:
# make sure collection_name and db_name is what you need
# keep feature: 'price_af_star'

db_name = 'MarketDataUpdate'
collection_name = 'HKEX_AF_Daily'

mng_client = pymongo.MongoClient(ip, 27017)
mng_db = mng_client[db_name]

db_cm = mng_db[collection_name]
myquery = { 'feature' : 'price_af'}
print('deleting...')

db_cm.delete_many(myquery)
print('deleting finished')
