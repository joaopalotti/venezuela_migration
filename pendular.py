import pandas as pd
import ast
import numpy as np
from misc import convert_unixtime, convert_country_code, get_slice
from misc import expand, get_item, extract_relationship, extract_education, extract_group
from misc import agebuckets
import sys
import os


def process_pendular(infile, results_dir="./results"):
	print("Processing %s" % (infile))

	df = pd.read_csv(infile)

	day, month, year = df["timestamp"].apply(lambda x: convert_unixtime(x)).head(1).values[0].split("-")
	print("Collection made on %s-%s-%s" % (day, month, year))

	def transform_dataframe(df):
	    df[["MinAge","MaxAge","Location","LocationHierarchy","LocationType","Gender","Relationship","Education","Group"]] = df["targeting"].apply(lambda x : expand(ast.literal_eval(x))).apply(pd.Series)

	    if "citizenship" in df:
		df["Group"] = df["citizenship"].fillna("[]").apply(lambda x : get_item(ast.literal_eval(x)))
		print("Updating Group information...")
	    elif "behavior" in df:
		df["Group"] = df["behavior"].fillna("[]").apply(lambda x : get_item(ast.literal_eval(x)))
		print("Updating Group information...")

	    if "access_device" in df:
		df["Device"] = df["access_device"].fillna("[]").apply(lambda x : get_item(ast.literal_eval(x)))
		print("Adding information regarding devices...")
	    else:
		df["Device"] = None

	    df["AgeBucket"] = df[["MinAge", "MaxAge"]].apply(lambda x: agebuckets(x["MinAge"], x["MaxAge"]), axis=1)

	    print("Removing redundant cols")
	    for col in ["Unnamed: 0", "all_fields", "targeting", 'behavior', 'citizenship', "mock_response", "access_device",
			"ages_ranges", "household_composition", 'interests', 'family_statuses', 'genders', 'geo_locations', 'languages',
			'name', 'relationship_statuses', 'response', 'scholarities', 'timestamp', 'publisher_platforms']:
		if col in df.keys():
		    del df[col]

	    print("All Done!")

	transform_dataframe(df)

	main_fields = ['demographics', 'MinAge', 'MaxAge', 'Location', 'LocationHierarchy', 'LocationType', 'Gender', 'Relationship', 'Education', 'Group', 'Device', 'AgeBucket']
	df = df.drop_duplicates(subset=main_fields)

	groups = ["Expats (Venezuela)", "Frequent international travellers", "FIT Venezuela", "FIT Non-expats", "Non-expats", "FIT Expat Non-Venezuelans", "Expat Non-Venezuelans"]

	frequency = "dau"

	dfgroups = {}
	dfcut = df[(df["Group"].apply(lambda x : x in groups)) & (df["AgeBucket"] == "all") & (df["LocationType"] == "home_recent")].copy()
	dfgroups["mau"] = get_slice(dfcut, "Group", groups, frequency="mau")
	dfgroups["dau"] = get_slice(dfcut, "Group", groups, frequency="dau")

	for freq in ["mau", "dau"]:
	    dfgroups[freq]["%FITV"] = dfgroups[freq]["audience_FIT Venezuela"] / dfgroups[freq]["audience_Expats (Venezuela)"]
	    dfgroups[freq]["%FITL"] = dfgroups[freq]["audience_FIT Non-expats"] / dfgroups[freq]["audience_Non-expats"]
	    dfgroups[freq]["%FITNV"] = dfgroups[freq]["audience_FIT Expat Non-Venezuelans"] / dfgroups[freq]["audience_Expat Non-Venezuelans"]

	concat = pd.concat((dfgroups["mau"],dfgroups["dau"])).reset_index(drop=True)
	merged = pd.merge(concat, df[["Location","LocationHierarchy"]].drop_duplicates())

	for trans_col in ["%FITV", "%FITL", "%FITNV"]:
	    merged[trans_col] = merged[trans_col] * 100.
	    merged[trans_col] = merged[trans_col].apply(lambda x: "%.2f" % x)

	merged.to_csv(os.path.join(results_dir, "pendular_%s_%s_%s.csv" % (year, month, day)), index=False, encoding="utf-8")
	print("Saved as " + os.path.join(results_dir, "pendular_%s_%s_%s.csv" % (year, month, day)))


if __name__ == "__main__":
     
    infile = sys.argv[1] # CSV file
    process_pendular(infile, "./results")
