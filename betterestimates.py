import pandas as pd
import numpy as np
import sys
import ast
from pysocialwatcher import watcherAPI

if len(sys.argv) < 1:
    print("%s <finished_collection>" % (sys.argv[0]))
    sys.exit(1)

infile = sys.argv[1]
countries_to_try = ["CR", "UY", "BO", "BR", "FR", "AR"]

# Should we stop re-issuing a query once we found a better estimate for it?
single_estimation = True

df = pd.read_csv(infile)

watcher = watcherAPI()
watcher.config(sleep_time=0, save_every=1000)
watcher.load_credentials_file("./credentials_masoomali.csv")

def prepare_to_reissue(df):
    df["response"] = None

def replace_by_country(row, country = "BR"):
    for option in ["countries", "regions", "cities"]:
        if option in row["geo_locations"]:
            del row["geo_locations"][option]
    row["geo_locations"]["countries"] = [country]
    return row

def add_country(row, country = "BR"):
    if "countries" in row["geo_locations"]:
        countries = row["geo_locations"]["countries"]
        if country not in countries:
            countries.append(country)
    else:
        countries = [country]

    row["geo_locations"]["countries"] = countries
    return row

def check_overlap(row):
    """
    Check overlap between regions. In the current version, only cities inside countries are checked.

    TODO: other regions like counties, zip codes and geo coordenates should be included in this function as well.
    """

    country_in_cities_field = set([])
    country_in_country_field = set([])

    if "cities" in row["geo_locations"]:
        for c in row["geo_locations"]["cities"]:
            country_in_cities_field.add(c["country"])

    if "countries" in row["geo_locations"]:
        for c in row["geo_locations"]["countries"]:
            country_in_country_field.add(c)

    # The same country was found in the list of countries and list of cities. Flag an error.
    if len(country_in_cities_field.intersection(country_in_country_field)) > 0:
        return None

    # if everything is okay, I just return the original row
    return row


df1000 = df.copy()
list_estimates = []
valid_estimate = [] # checks if a new estimate is in between 1001 and 9999.

queries_need_better_estimate = df[df["mau_audience"] <= 1000].shape[0]
total_queries = df.shape[0]

print("We found %d (%.3f) queries in the input file have an estimated audience of 1000 people. Trying to find better estimates using the following countries (%s)" %
        (queries_need_better_estimate, 1.0 * queries_need_better_estimate / total_queries, countries_to_try))

for country in countries_to_try:

    print ("USING COUNTRY: ", country)
    df1000 = df1000[df1000["mau_audience"] <= 1000].copy()

    if single_estimation:
        # In this case, we should avoid re-issueing queries that we already have a better valid estimation
        if len(valid_estimate) > 0:
            tmpvalid = pd.DataFrame(valid_estimate).T
            tmpvalid = tmpvalid.any(axis=1)
            # Issues queries only to the ones that we do not have info yet
            df1000 = df1000[~tmpvalid]

    df1000_in_country = df1000.copy(deep=True)
    df1000_add_country = df1000.copy(deep=True)

    prepare_to_reissue(df1000_in_country)
    prepare_to_reissue(df1000_add_country)

    # Transform string into json.
    df1000_in_country["targeting"] = df1000_in_country["targeting"].apply(lambda x: ast.literal_eval(x))
    df1000_add_country["targeting"] = df1000_add_country["targeting"].apply(lambda x: ast.literal_eval(x))

    # Modifies "targeting" to include country
    df1000_add_country["targeting"].apply(lambda x: add_country(x, country))
    df1000_in_country["targeting"].apply(lambda x: replace_by_country(x, country))

    # Check if region is inside country. If it is, return None
    df1000_add_country["targeting"] = df1000_add_country["targeting"].apply(lambda x: check_overlap(x))

    # Remove rows which targeting is invalid.
    df1000_add_country = df1000_add_country[~df1000_add_country["targeting"].isnull()]
    df1000_in_country = df1000_in_country[~df1000_in_country["targeting"].isnull()]

    # This is the case in which we do not have any new query to issue. Just continue to the next country
    if df1000_add_country.empty:
        continue

    try:
        res_in_country = watcherAPI.perform_collection_data_on_facebook(df1000_in_country)
        res_add_country = watcherAPI.perform_collection_data_on_facebook(df1000_add_country)

    except Exception as err:
        # We get an error when we OR a region and a country if the region is inside the country.
        # e.g., region = doha, country = QA -> Error: Some of your locations overlap. Try removing a location.
        # We just ignore this country in our estimates
        print("Found the following error: {0}".format(err))
        print("Ignoring country %s and continuing..." % (country))
        continue

    #print("in country:", df1000_in_country["mau_audience"])
    #print("add country:", df1000_add_country["mau_audience"])
    #print(df1000_add_country["mau_audience"] - df1000_in_country["mau_audience"])

    # We first check if these estimates are good... Both datasets need to have more than 1000 and less than 10000
    valid = (df1000_in_country["mau_audience"] > 1000) & (df1000_in_country["mau_audience"] < 10000) &\
            (df1000_add_country["mau_audience"] > 1000) & (df1000_add_country["mau_audience"] < 10000) &\
            (df1000_add_country["mau_audience"] >= df1000_in_country["mau_audience"])
    valid_estimate.append(valid)

    valid_add = df1000_add_country[valid]
    valid_in = df1000_in_country[valid]
    
    estimates = valid_add["mau_audience"] - valid_in["mau_audience"]
    estimates.name = country

    list_estimates.append(estimates)

# We say we got a valid estimate for a row if any auxiliary estimate is valid
valid = pd.DataFrame(valid_estimate).T
valid = valid.any(axis=1)

res = pd.DataFrame(list_estimates).T
result = res.mean(axis=1)

if result.shape[0] != queries_need_better_estimate:
    print("WARNIG: We could not find estimators for all the regions. Try increasing the number of countries used as input. Currently using %s" % (countries_to_try))
    #print("Exiting without saving output file...")
    #sys.exit(1)

still_can_get_better_estimates = valid[~valid]

if not still_can_get_better_estimates.empty:
    print("Found better estimates to %d queries." % (result.shape[0]))
    print("There are still %d queries that we could not find any valid estimate." % (still_can_get_better_estimates.shape[0]))
    print("Take a look at these indices: ", (still_can_get_better_estimates.index.values))

# These are the queries that we can safely replace in the input dataset
df.loc[result.index, "mau_audience"] = result

savefile = "%s.betterestimate" % (infile)
df.to_csv(savefile)

print("Saved better estimates for %d out of %d queries in file '%s'..." % (result.shape[0], queries_need_better_estimate, savefile))
print("Done.")

