from pysocialwatcher import watcherAPI
import os
import sys
import json
import pandas as pd

from pysocialwatcher import constants
import requests
requests.packages.urllib3.disable_warnings()

constants.SLEEP_TIME = 0
constants.SAVE_EVERY = 5000

credential_file = sys.argv[1]

watcher = watcherAPI()
#watcher.load_credentials_file("/home/local/QCRI/jpalotti/github/venezuela_migration/credentials.csv")
#watcher.load_credentials_file("/home/local/QCRI/jpalotti/github/venezuela_migration/credentials_masoomali.csv")
watcher.load_credentials_file(credential_file)

df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/jsons/main_collection.json")

print("UNIQUE_TIME_ID:", constants.UNIQUE_TIME_ID)

go = "./go.sh dataframe_collected_finished_%s.csv" % (constants.UNIQUE_TIME_ID)
print(go)
os.system(go)

