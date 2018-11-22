from pysocialwatcher import watcherAPI

import json
import pandas as pd
import re, sys, os

from pysocialwatcher import constants
import requests
requests.packages.urllib3.disable_warnings()

infile = sys.argv[1]

watcher = watcherAPI()
watcherAPI.config(sleep_time=0, save_every=10000)

constants.TOKENS = []

utime = re.findall(r'\d+', infile)[0]
constants.UNIQUE_TIME_ID = utime

constants.DATAFRAME_SKELETON_FILE_NAME = "dataframe_skeleton_" + constants.UNIQUE_TIME_ID + ".csv"
constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME = "dataframe_collecting_" + constants.UNIQUE_TIME_ID + ".csv"
constants.DATAFRAME_AFTER_COLLECTION_FILE_NAME = "dataframe_collected_finished_" + constants.UNIQUE_TIME_ID + ".csv"
constants.DATAFRAME_AFTER_COLLECTION_FILE_NAME_WITHOUT_FULL_RESPONSE = "collect_finished_clean" + constants.UNIQUE_TIME_ID + ".csv"

watcher.load_credentials_file("/home/local/QCRI/jpalotti/github/venezuela_migration/credentials_masoomali.csv")
watcher.load_data_and_continue_collection(infile)

go = "./go.sh dataframe_collected_finished_%s.csv" % (utime)
print(go)
os.system(go)

