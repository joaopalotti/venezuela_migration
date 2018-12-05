from pysocialwatcher import watcherAPI

import sys
import json
import pandas as pd

import os

import requests
from pysocialwatcher import constants

requests.packages.urllib3.disable_warnings()

option = int(sys.argv[1])
credential_file = None if len(sys.argv) <= 1 else sys.argv[2]

watcherAPI.config(sleep_time=0, save_every=2000)

watcher = watcherAPI()

if not credential_file:
    watcher.load_credentials_file("/home/local/QCRI/jpalotti/github/venezuela_migration/credentials.csv")
else:
    watcher.load_credentials_file(credential_file)

stropt = ["", "married", "pendular", "devices", "devices_ven", "immigrants", "colombia_short", "immigrants_global", "syrians", "boavista", "manaus"]

if option == 1:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/married.json")
elif option == 2:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/pendular.json")
elif option == 3:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/devices.json")
elif option == 4:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/venezuelans_devices.json")
elif option == 5:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/immigrants.json")
elif option == 6:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/main_collection_short.json")
elif option == 7:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/immigrants_global.json")
elif option == 8:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/syrians.json")
elif option == 9:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/boavista.json")
elif option == 10:
    df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/manaus.json")

time = constants.UNIQUE_TIME_ID
print("UNIQUE_TIME_ID: %s" % (time) )
print("Finished collection for %s" % (stropt[option]))

fixdataframe = "./fix_dataframe.sh dataframe_collected_finished_%s.csv corrects/correct_%s.csv" % (time, stropt[option])
gziping = "gzip dataframe_collected_finished_%s.csv" % (time)
mvcollection = "mv dataframe_collected_finished_%s.csv.gz collections/%s" % (time, stropt[option])
gitadd = "git add collections/%s/dataframe_collected_finished_%s.csv.gz" % (stropt[option], time)

print(fixdataframe)
os.system(fixdataframe)

print(gziping)
os.system(gziping)

print(mvcollection)
os.system(mvcollection)

print(gitadd)
os.system(gitadd)
