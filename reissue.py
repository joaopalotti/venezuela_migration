
from pysocialwatcher import watcherAPI

import json
import pandas as pd
import re, sys

from pysocialwatcher import constants
infile = sys.argv[1]


watcher = watcherAPI()

constants.TOKENS = []
constants.SLEEP_TIME = 6

#utime = re.findall(r'\d+', infile)[0]
#constants.UNIQUE_TIME_ID = utime

constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME = infile
constants.DATAFRAME_AFTER_COLLECTION_FILE_NAME = infile
constants.DATAFRAME_AFTER_COLLECTION_FILE_NAME_WITHOUT_FULL_RESPONSE = "collect_finished_clean" + infile + ".csv"
constants.SAVE_EVERY = 20

watcher.load_credentials_file("/home/jpalotti/github/venezuela_migration/credentials.csv")

watcher.load_data_and_continue_collection(infile)


