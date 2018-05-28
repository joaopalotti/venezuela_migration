from pysocialwatcher import watcherAPI

import json
import pandas as pd

from pysocialwatcher import constants
#constants.SLEEP_TIME = 10

watcher = watcherAPI()
watcher.load_credentials_file("/home/local/QCRI/jpalotti/github/venezuela_migration/credentials.csv")

df = watcher.run_data_collection("/home/local/QCRI/jpalotti/github/venezuela_migration/main_collection_short.json")

print("UNIQUE_TIME_ID:", constants.UNIQUE_TIME_ID)
