import pandas as pd
import numpy as np
import ast
import datetime
from misc import display, convert_unixtime, convert_country_code, cut, copy_rename, get_slice, calculate_percentages
from misc import expand, extract_relationship, extract_education, extract_group, agebuckets, get_item
import os, sys

def process_immigrants(infile, results_dir="./results"):
	df = pd.read_csv(infile)

	day, month, year = df["timestamp"].apply(lambda x: convert_unixtime(x)).head(1).values[0].split("-")
	print("Collection made on %s-%s-%s" % (day, month,year))

	df[["MinAge","MaxAge","Location","LocationHierarchy","LocationType","Gender","Relationship","Education","Group"]] = df["targeting"].apply(lambda x : expand(ast.literal_eval(x))).apply(pd.Series)

	if "citizenship" in df:
	    df["Group"] = df["citizenship"].fillna("[]").apply(lambda x : get_item(ast.literal_eval(x)))
	    print("Updating Group information...")

	if "access_device" in df:
	    df["Device"] = df["access_device"].fillna("[]").apply(lambda x : get_item(ast.literal_eval(x)))
	    print("Adding information regarding devices...")
	else:
	    df["Device"] = None

	df["AgeBucket"] = df[["MinAge","MaxAge"]].apply(lambda x: agebuckets(x["MinAge"], x["MaxAge"]), axis=1)

	print("Removing redundant cols")
	for col in ["Unnamed: 0", "all_fields", "targeting",'behavior', 'citizenship', "mock_response", "access_device",
		    "ages_ranges", "household_composition", 'interests', 'family_statuses', 'genders', 'geo_locations', 'languages',
		    'name', 'relationship_statuses', 'response', 'scholarities', 'timestamp', 'publisher_platforms',]:
	    if col in df.keys():
		del df[col]

	# Gave up using travel_in
	df = df[df["LocationType"] != "travel_in"]

	print("All Done!")

	dfgender = {}
	dfcut = df[(df["Gender"].apply(lambda x : x in ["man","woman"])) & (df["Education"].isnull()) & (df["Device"].isnull()) & (df["AgeBucket"] == "all") & (df["Group"] == "Expats (Venezuela)") & (df["LocationType"] == "home_recent")].copy()
	dfgender["mau"] = get_slice(dfcut, "Gender", ["man","woman"], frequency="mau")
	dfgender["dau"] = get_slice(dfcut, "Gender", ["man","woman"], frequency="dau")
	calculate_percentages(dfgender["mau"], ["man","woman"], "%")
	calculate_percentages(dfgender["dau"], ["man","woman"], "%")

	df_gender_locals = {}
	dfcut = df[(df["Gender"].apply(lambda x : x in ["man","woman"])) & (df["Education"].isnull()) & (df["Device"].isnull())
		   & (df["AgeBucket"] == "all") & (df["Group"].isnull()) & (df["LocationType"] == "home_recent") ].copy()

	df_gender_locals["mau"] = get_slice(dfcut, "Gender", ["man","woman"], frequency="mau")
	df_gender_locals["dau"] = get_slice(dfcut, "Gender", ["man","woman"], frequency="dau")

	calculate_percentages(df_gender_locals["mau"], ["man","woman"], "%locals_")
	calculate_percentages(df_gender_locals["dau"], ["man","woman"], "%locals_")

	for col in ["man","woman"]:
	    del df_gender_locals["mau"]["audience_" + col]
	    del df_gender_locals["dau"]["audience_" + col]


	def get_top_going_to_aux(n, df, group):

	    colname = 'audience_Expats (' + group + ')'

	    if colname in df:
		idx = df[colname].nlargest(n).index
		locations = df.iloc[idx][["Location"]].T
		values = df.iloc[idx][[colname]].T
	    else:
		locations = pd.DataFrame(['-'] * nlargest, index=range(1, nlargest+1)).T
		values = pd.DataFrame(['-'] * nlargest, index=range(1, nlargest+1)).T

	    locations.index = [group]
	    values.index = [group]
	    return locations, values

	def get_top_going_to(n, df):
	    acc_loc, acc_val = [], []
	    for loc in  df_immi_place["mau"]["Location"]:
		toAppend_loc, toAppend_val = get_top_going_to_aux(nlargest, df, loc)
		toAppend_loc.columns = ['goingTo{}'.format(i) for i in range(1, nlargest+1)]
		toAppend_val.columns = ['goingTo{}_value'.format(i) for i in range(1, nlargest+1)]


		acc_loc.append(toAppend_loc)
		acc_val.append(toAppend_val)

	    toAppend_val = pd.concat(acc_val)
	    toAppend_loc = pd.concat(acc_loc)
	    return toAppend_val, toAppend_loc


	immigrant_groups = [x for x in df["Group"].unique() if x not in [None, "All - Expats"]]

	df_immi_place = {}
	dfcut = df[(df["Group"].apply(lambda x: x in immigrant_groups)) & (df["Gender"] == "both") & (df["Education"].isnull()) &
		   (df["Device"].isnull()) & (df["AgeBucket"] == "all") & (df["LocationType"] == "home_recent")].copy()
	df_immi_place["mau"] = get_slice(dfcut, "Group", immigrant_groups, frequency="mau")
	df_immi_place["dau"] = get_slice(dfcut, "Group", immigrant_groups, frequency="dau")

	calculate_percentages(df_immi_place["mau"], immigrant_groups, "%")
	calculate_percentages(df_immi_place["dau"], immigrant_groups, "%")

	dfcut = df[df["Group"].isnull()]

	loc_mau = dfcut[["Location", "mau_audience"]].rename(columns={"mau_audience": "audience"})
	loc_dau = dfcut[["Location", "dau_audience"]].rename(columns={"dau_audience": "audience"})

	loc_mau["Frequency"] = "Monthly"
	loc_dau["Frequency"] = "Daily"

	df_countrypop = pd.concat((loc_mau,loc_dau))

	# Generated dffinal
	dffinal = pd.concat([df_immi_place["mau"], df_immi_place["dau"]])
	dffinal = pd.merge(dffinal, df_countrypop, on=["Location","Frequency"])

	# Add aggregated stats for total number of immigrants
	audience_group = []
	for group in immigrant_groups:
	    audience_group += ["audience_" + group]

	dffinal["summary_total_immigrants"] = dffinal[audience_group].sum(axis=1)
	dffinal["immigrants_in_population"] = (100. * dffinal["summary_total_immigrants"] / dffinal["audience"])
	dffinal["immigrants_in_population"] = dffinal["immigrants_in_population"].apply(lambda x: "%.1f%%" % x)

	dffinal["display_total_immigrants"] = dffinal["summary_total_immigrants"].fillna(0).apply(lambda x: display(x))

	nlargest = 5

	for col in dffinal.keys():
	    if col.startswith("%"):
		#print(col)
		dffinal[col] = dffinal[col].round(2)

	# Add TOP 'nlargest' population counts:
	audiences = [k for k in dffinal.keys() if k.startswith("audience_")]
	dfaud = dffinal[audiences]

	def cleanCoutryName(name):
	    return " ".join(name.rsplit()[1:]).strip("()")

	order = np.argsort(-dfaud.values, axis=1)[:, :nlargest]
	names = pd.DataFrame(dfaud.columns[order], columns=['comeFrom{}'.format(i) for i in range(1, nlargest+1)],
			      index=dfaud.index)

	for k in names.keys():
	    names[k] = names[k].apply(lambda x: cleanCoutryName(x))

	values = np.abs(np.sort(-dfaud.values, axis=1)[:,:nlargest])
	results = pd.DataFrame(values, columns=['comeFrom{}_value'.format(i) for i in range(1, nlargest+1)],
			      index=dfaud.index)

	dffinal = pd.concat((dffinal, names, results), axis=1)

	dffinal.to_csv(os.path.join(results_dir, "immigrants_%s_%s_%s.csv" % (year, month, day)), index=False, encoding="utf-8")
	print("Saved as " + os.path.join(results_dir, "immigrants_%s_%s_%s.csv" % (year, month, day)))


if __name__ == "__main__":

	infile = sys.argv[1] # CSV file
	results_dir = "./results"
	process_immigrants(infile, results_dir)
	



