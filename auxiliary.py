import time
import sys
import os
import requests
from pysocialwatcher import watcherAPI
from pysocialwatcher import constants
from pysocialwatcher import utils
import pandas as pd

localdir = "/home/local/QCRI/jpalotti/venezuela/"

requests.packages.urllib3.disable_warnings()

option = int(sys.argv[1])
credential_file = os.path.join(localdir, "credentials_masoomali.csv") if len(sys.argv) <= 2 else sys.argv[2]

watcherAPI.config(sleep_time=0, save_every=10000)

watcher = watcherAPI()
watcher.load_credentials_file(credential_file)

stropt = ["", "married", "pendular", "devices", "devices_ven", "immigrants", "colombia_short", "immigrants_global", "syrians", "boavista", "manaus",
		"pibbrasil", "immigrants_unchr", "superdevices", "sp_vs_nonsp", "ven_in_us", "ven_in_venstates", "georgians", "small_business"]

def mycollection(json_input_file_path, BATCH_SIZE=20000):
    input_data_json = watcherAPI.read_json_file(json_input_file_path)
    watcherAPI.expand_input_if_requested(input_data_json)
    watcherAPI.check_input_integrity(input_data_json)
    df = watcherAPI.build_collection_dataframe(input_data_json)

    original_name = constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME
    iteration = 0
    print("Total API calls to be made: %d. Dividing it in to batch of size %d. Divided into %d batches..." % (df.shape[0], BATCH_SIZE, df.shape[0]/BATCH_SIZE))

    results = []
    while not df.empty:
        print("Running BATCH %d. " % (iteration))
        iteration += 1
        df_batch = df[:BATCH_SIZE].copy()
        if not df_batch.empty:
	    try:
            	constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME = original_name + "_%d" % (iteration)
            	result_batch = watcherAPI.perform_collection_data_on_facebook(df_batch)
            	results.append(result_batch)
	    except:
		print("Got error...lets sleep for a minute...")
		time.sleep(60)
		iteration -= 1
		continue

        df = df[BATCH_SIZE:]

    constants.DATAFRAME_TEMPORARY_COLLECTION_FILE_NAME = original_name
    df = pd.concat(results)
    df = utils.post_process_collection(df)
    utils.save_after_collecting_dataframe(df)
    return df


if option == 1:
    df = mycollection(os.path.join(localdir,"./jsons/married.json"))
elif option == 2:
    df = mycollection(os.path.join(localdir,"./jsons/pendular.json"))
elif option == 3:
    df = mycollection(os.path.join(localdir,"./jsons/devices.json"))
elif option == 4:
    df = mycollection(os.path.join(localdir,"./jsons/venezuelans_devices.json"))
elif option == 5:
    df = mycollection(os.path.join(localdir,"./jsons/immigrants.json"))
elif option == 6:
    df = mycollection(os.path.join(localdir,"./jsons/main_collection_short.json"))
elif option == 7:
    df = mycollection(os.path.join(localdir,"./jsons/immigrants_global.json"))
elif option == 8:
    df = mycollection(os.path.join(localdir,"./jsons/syrians.json"))
elif option == 9:
    df = mycollection(os.path.join(localdir,"./jsons/boavista.json"))
elif option == 10:
    df = mycollection(os.path.join(localdir,"./jsons/manaus.json"))
elif option == 11:
    df = mycollection(os.path.join(localdir,"./jsons/pibbrasil.json"))
elif option == 12:
    df = mycollection(os.path.join(localdir,"./jsons/immigrants_global_unhcr.json"))
elif option == 13:
    df = mycollection(os.path.join(localdir,"./jsons/superdevices.json"))
elif option == 14:
    df = mycollection(os.path.join(localdir,"./jsons/sp_vs_nonsp.json"))
elif option == 15:
    df = mycollection(os.path.join(localdir,"./jsons/ven_in_us.json"))
elif option == 16:
    df = mycollection(os.path.join(localdir,"./jsons/ven_in_venstates.json"))
elif option == 17:
    df = mycollection(os.path.join(localdir,"./jsons/georgians.json"))
elif option == 18:
    df = mycollection(os.path.join(localdir,"./jsons/small_business.json"))

time = constants.UNIQUE_TIME_ID
print("UNIQUE_TIME_ID: %s" % (time) )
print("Finished collection for %s" % (stropt[option]))

fixdataframe = "%s/fix_dataframe.sh dataframe_collected_finished_%s.csv %s/corrects/correct_%s.csv" % (localdir, time, localdir, stropt[option])
gziping = "gzip dataframe_collected_finished_%s.csv" % (time)
mvcollection = "mv dataframe_collected_finished_%s.csv.gz %s/collections/%s" % (time, localdir, stropt[option])
#gitadd = "git add %s/collections/%s/dataframe_collected_finished_%s.csv.gz" % (localdir, stropt[option], time)

print(fixdataframe)
os.system(fixdataframe)

print(gziping)
os.system(gziping)

print(mvcollection)
os.system(mvcollection)

#print(gitadd)
#os.system(gitadd)


# Post-processing:
if option == 1:
    from married import process_married
    process_married("%s/collections/%s/dataframe_collected_finished_%s.csv.gz" % (localdir, stropt[option], time))

elif option == 2:
    from pendular import process_pendular
    process_pendular("%s/collections/%s/dataframe_collected_finished_%s.csv.gz" % (localdir, stropt[option], time))

elif option == 5:
    from immigrants import process_immigrants
    process_immigrants("%s/collections/%s/dataframe_collected_finished_%s.csv.gz" % (localdir, stropt[option], time))

