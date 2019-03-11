from pysocialwatcher import watcherAPI
from pysocialwatcher import utils
import os
import sys
import json
import pandas as pd

from pysocialwatcher import constants
import requests
requests.packages.urllib3.disable_warnings()

watcherAPI.config(sleep_time=0, save_every=5000)

credential_file = None if len(sys.argv) <= 1 else sys.argv[1]

watcher = watcherAPI()
if not credential_file:
    watcher.load_credentials_file("/home/local/QCRI/jpalotti/github/venezuela_migration/credentials.csv")
else:
    watcher.load_credentials_file(credential_file)

def my_collection(json_input_file_path, BATCH_SIZE=200):
    input_data_json = watcherAPI.read_json_file(json_input_file_path)
    watcherAPI.expand_input_if_requested(input_data_json)
    watcherAPI.check_input_integrity(input_data_json)
    df = watcherAPI.build_collection_dataframe(input_data_json)

    original_name = constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME
    iteration = 0

    results = []
    while not df.empty:
        print("Running BATCH %d. " % (iteration))
        iteration += 1
        df_batch = df[:BATCH_SIZE].copy()
        if not df_batch.empty:
            constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME = original_name + "_%d" % (iteration)
            result_batch = watcherAPI.perform_collection_data_on_facebook(df_batch)
            results.append(result_batch)

        df = df[BATCH_SIZE:]

    constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME = original_name
    df = pd.concat(results)
    df = utils.post_process_collection(df)
    utils.save_after_collecting_dataframe(df)
    return df

df = my_collection("./jsons/main_collection.json")
#df = watcher.run_data_collection("./jsons/main_collection.json")

print("UNIQUE_TIME_ID:", constants.UNIQUE_TIME_ID)

go = "./go.sh dataframe_collected_finished_%s.csv" % (constants.UNIQUE_TIME_ID)
print(go)
os.system(go)

