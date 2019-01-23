import sys
import os
import requests
from pysocialwatcher import watcherAPI
from pysocialwatcher import constants

requests.packages.urllib3.disable_warnings()

option = int(sys.argv[1])
credential_file = "./credentials_masoomali.csv" if len(sys.argv) <= 2 else sys.argv[2]

watcherAPI.config(sleep_time=0, save_every=10000)

watcher = watcherAPI()
watcher.load_credentials_file(credential_file)

stropt = ["", "married", "pendular", "devices", "devices_ven", "immigrants", "colombia_short", "immigrants_global", "syrians", "boavista", "manaus", "pibbrasil", "immigrants_unchr", "superdevices"]

if option == 1:
    df = watcher.run_data_collection("./jsons/married.json")
elif option == 2:
    df = watcher.run_data_collection("./jsons/pendular.json")
elif option == 3:
    df = watcher.run_data_collection("./jsons/devices.json")
elif option == 4:
    df = watcher.run_data_collection("./jsons/venezuelans_devices.json")
elif option == 5:
    df = watcher.run_data_collection("./jsons/immigrants.json")
elif option == 6:
    df = watcher.run_data_collection("./jsons/main_collection_short.json")
elif option == 7:
    df = watcher.run_data_collection("./jsons/immigrants_global.json")
elif option == 8:
    df = watcher.run_data_collection("./jsons/syrians.json")
elif option == 9:
    df = watcher.run_data_collection("./jsons/boavista.json")
elif option == 10:
    df = watcher.run_data_collection("./jsons/manaus.json")
elif option == 11:
    df = watcher.run_data_collection("./jsons/pibbrasil.json")
elif option == 12:
    df = watcher.run_data_collection("./jsons/immigrants_global_unhcr.json")
elif option == 13:
    df = watcher.run_data_collection("./jsons/superdevices.json")

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
