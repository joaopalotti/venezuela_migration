import pandas as pd
import ast
import datetime
from misc import display, convert_unixtime, convert_country_code, cut, copy_rename, get_slice, calculate_percentages
from misc import expand, extract_relationship, extract_education, extract_group, agebuckets, get_item
import os, sys

def process_married(infile, results_dir = "./results"):

    df = pd.read_csv(infile)
    day, month, year = df["timestamp"].apply(lambda x: convert_unixtime(x)).head(1).values[0].split("-")
    print("Collection made on %s-%s-%s" % (day, month,year))

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

        df["AgeBucket"] = df[["MinAge","MaxAge"]].apply(lambda x: agebuckets(x["MinAge"], x["MaxAge"]), axis=1)

        print("Removing redundant cols")
        for col in ["Unnamed: 0", "all_fields", "targeting",'behavior', 'citizenship', "mock_response", "access_device", 
            "ages_ranges", "household_composition", 'interests', 'family_statuses', 'genders', 'geo_locations', 'languages',
            'name', 'relationship_statuses', 'response', 'scholarities', 'timestamp', 'publisher_platforms',]:
            if col in df.keys():
                del df[col]

        print("All Done!")

    transform_dataframe(df)

    main_fields = ['demographics', 'MinAge', 'MaxAge', 'Location', 'LocationHierarchy', 'LocationType', 'Gender', 'Relationship', 'Education', 'Group', 'Device', 'AgeBucket']
    df = df.drop_duplicates(subset=main_fields)

    relationships = ["single","dating","married"]

    def relationship_distribution(df, group):
        if group == "Expats (Venezuela)":
            label = "ven_"
        elif group == "Non-expats":
            label = "locals_"
        else:
            print("Group %s is not recognized!" % (group))

        dfrelationship = {}
        dfcut = df[(df["Relationship"].apply(lambda x : x in relationships)) & (df["AgeBucket"] == "all") & (df["LocationType"] == "home_recent") & (df["Group"] == group) ].copy()
        dfrelationship["mau"] = get_slice(dfcut, "Relationship", relationships, frequency="mau")
        dfrelationship["dau"] = get_slice(dfcut, "Relationship", relationships, frequency="dau")

        calculate_percentages(dfrelationship["mau"], relationships, "%" + label)
        calculate_percentages(dfrelationship["dau"], relationships, "%" + label)

        concated = pd.concat((dfrelationship["mau"],dfrelationship["dau"])).reset_index(drop=True)
        for rel in relationships:
            concated["audience_" + label + rel] = concated["audience_" + rel] 
            del concated["audience_" + rel]

        return concated

    dfven = relationship_distribution(df, "Expats (Venezuela)")
    dflocal = relationship_distribution(df, "Non-expats")
    merged = pd.merge(dfven, dflocal)

    merged = pd.merge(merged, df[["Location","LocationHierarchy"]].drop_duplicates())

    for rel in relationships:
        merged["%diff_audience_" + rel ] = merged["%ven_audience_" + rel] - merged["%locals_audience_" + rel] 


    for trans_col in [k for k in merged.keys() if k.startswith("%")]:
        merged[trans_col] = merged[trans_col].apply(lambda x: "%.2f" % x)

    for key in merged.keys():
        if key.startswith("audience_"):
            merged["display_"+ key] = merged[key].apply(lambda x: display(x))

    merged.to_csv(os.path.join(results_dir, "married_%s_%s_%s.csv" % (year, month, day)), index=False, encoding="utf-8")
    print("Saved as " + os.path.join(results_dir, "married_%s_%s_%s.csv" % (year, month, day)))

if __name__ == "__main__":

	infile = sys.argv[1] # CSV file
	results_dir = "./results"
	process_married(infile, results_dir)
	

