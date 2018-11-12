import pandas as pd
import numpy as np
import sys, os
import ast
from pysocialwatcher import watcherAPI

if len(sys.argv) < 1:
    print("%s <finished_collection>" % (sys.argv[0]))
    os.exit(1)

infile = sys.argv[1]
countries_to_try = ["KE", "AU", "JP", "BR", "CO"]

# Should we stop re-issuing a query once we found a better estimate for it?
single_estimation = True

df = pd.read_csv(infile)

watcher = watcherAPI()
watcher.config(sleep_time=0, save_every=1000)
watcher.load_credentials_file("./credentials.csv")

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

    print("in country:", df1000_in_country["mau_audience"])
    print("add country:", df1000_add_country["mau_audience"])
    print(df1000_add_country["mau_audience"] - df1000_in_country["mau_audience"])

    estimates = df1000_add_country["mau_audience"] - df1000_in_country["mau_audience"]
    estimates.name = country

    list_estimates.append(estimates)

    # if estimate is 0, it might be because of two reasons:
    # (1) the estimate is lost in the precision for the country.
    # That happens when the query finds more that 10k people for a given query in a country. All that we can do about it is to ignore this country and try another one.
    # (2) the estimate is really 0. That happens when the query finds less than 10k people. In this situation it does not matter if we change the country, we will still get a 0.
    # We save the results to know if we should flag an error or just informe that the audience is really 0.
    valid_estimate.append((df1000_in_country["mau_audience"] > 1000) & (df1000_in_country["mau_audience"] < 10000))

res = pd.DataFrame(list_estimates).T
result = res.replace(0, np.nan).mean(axis=1)

if result.shape[0] != queries_need_better_estimate:
    print("ERROR: We could not find estimators for all the regions. Try increasing the number of countries used as input. Currently using %s" % (countries_to_try))
    print("Exiting without saving output file...")
    os.exit(1)

# If any row of less10k is True, we can say that a 0 means that 0 people were in the location for a given query.
valid = pd.DataFrame(valid_estimate).T
valid = valid.any(axis=1)

still_can_get_better_estimates = result[((result == 0) | (result.isnull())) & (~valid)]

if not still_can_get_better_estimates.empty:
    print("There are still %d queries that we could not find better estimates." % (still_can_get_better_estimates.shape[0]))
    print("Take a look at these indices: ", (still_can_get_better_estimates.index.values))

# These are the queries that we can safely replace in the input dataset
final_result = result[valid]
df.loc[final_result.index, "mau_audience"] = final_result

savefile = "%s.betterestimate" % (infile)
df.to_csv(savefile)

print("Saved better estimates for %d out of %d queries in file '%s'..." % (final_result.shape[0], queries_need_better_estimate, savefile))
print("Done.")



