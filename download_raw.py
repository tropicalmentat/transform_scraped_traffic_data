from google.cloud import storage
import os
import logging
import datetime as dt
import sys

data_fpath = r'data'

client = storage.Client.from_service_account_json(os.environ['GCLOUD_STORAGE_CREDS'])

bucket = client.get_bucket('mmda-tv5-scrape-dumps')

# get yesterday's data
yesterday = (dt.datetime.now() - dt.timedelta(1)).strftime("%Y%m%d")

blobs = list(bucket.list_blobs(prefix=yesterday))

try:
	os.mkdir(os.path.join(data_fpath,yesterday))
except Exception as e:
	print(e)

dump = list()

for blb in blobs:
	blb_name = blb.name
	fname = blb.name.split('/')[1]
	
	with open(data_fpath+'/'+yesterday+'/'+fname+'.txt',"wb") as fobj:
		# fobj.writelines(data)
		client.download_blob_to_file(blb,fobj)

# print(sys.getsizeof(dump))

# TODO: Explore and understand data
# when does a line status get changed?
# how do we get the unique status at a particular snapshot?