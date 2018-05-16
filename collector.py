from pysocialwatcher import watcherAPI
import json
import pandas as pd

watcher = watcherAPI()
watcher.load_credentials_file("credentials.csv")
df = watcher.run_data_collection("./venezuelans_in_brazil.json")


