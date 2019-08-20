import pandas as pd
import sys
import ast
import json
from pysocialwatcher import watcherAPI

# To avoid warinings from old python instalations
import requests
requests.packages.urllib3.disable_warnings()

usingCache = True
expiration_in_sec = 432000
# Some options:
# 5 days -> 432000
# 30 days -> 2.592e+6

if usingCache:
    # To create a cache that verifies if a query is useless
    import redis
    r = redis.Redis(host='localhost', port=6379, db=1)
    print("Using Redis as a cache to avoid unfruitful API calls.")

if len(sys.argv) <= 1:
    print("%s <finished_collection>" % (sys.argv[0]))
    sys.exit(1)

infile = sys.argv[1]
#countries_to_try = ["CR" , "UY", "FR", "JP", "BO", "CL", "PA", "US", "ES", "BR", "AR", "PT", "CA", "PE", "EC", "MX", "NI", "DE"] 
#countries_to_try = ["PA", "US", "ES"] 
countries_to_try = ["CR"]
#countries_to_try = ["BR"]

# Should we stop re-issuing a query once we found a better estimate for it?
single_estimation = True

df = pd.read_csv(infile)

watcher = watcherAPI()
watcher.config(sleep_time=0, save_every=5000)
watcher.load_credentials_file("./credentials_masoomali.csv")

result = None

def prepare_to_reissue(df):
    df["response"] = None

"""
TODO: Proably the best way is to copy the dictionary 'row' and return a new object.
"""
def replace_by_country(row, country = "BR"):
    for option in ["countries", "regions", "cities", "custom_locations"]:
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

    TODO: missing other regions such as zip codes.
    """
    country_in_location_field = set([])
    country_in_country_field = set([])

    if "cities" in row["geo_locations"]:
        for c in row["geo_locations"]["cities"]:
            country_in_location_field.add(c["country"])
    
    if "custom_locations" in row["geo_locations"]:
        for c in row["geo_locations"]["custom_locations"]:
	    country_in_location_field.add(c["country"])
    
    if "regions" in row["geo_locations"]:
        for c in row["geo_locations"]["regions"]:
	    if "country_code" in c:
		country_in_location_field.add(c["country_code"])
	    elif "country" in c:
		country_in_location_field.add(c["country"])

    if "countries" in row["geo_locations"]:
        for c in row["geo_locations"]["countries"]:
            country_in_country_field.add(c)


    # The same country was found in the list of countries and list of cities. Flag an error.
    if len(country_in_location_field.intersection(country_in_country_field)) > 0:
        return None

    # if everything is okay, I just return the original row
    return row

def save_partial_results(df, result, infile):

    res = pd.DataFrame(list_estimates).T
    result = res.mean(axis=1)

    # These are the queries that we can safely replace in the input dataset
    df.loc[result.index, "mau_audience"] = result

    savefile = "%s.betterestimate" % (infile)
    df.to_csv(savefile + ".gz", compression='gzip', index=False)

    found_better_estimate = (result < 1000).sum()
    still_can_get_better_estimates = df["mau_audience"].isnull().sum()
    
    print("Saved better estimates for %d queries in file '%s'..." % (found_better_estimate, savefile + ".gz"))
    print("Still missing to find better estimatives to %d (%.3f) queries..." % ( still_can_get_better_estimates, 1. * (still_can_get_better_estimates) /df.shape[0]))
    print("Saved partial results to file '%s'" % (savefile+".gz"))
    return result

def is_bad_country(key, country):
    countries = str_to_set(r.get(key))
    if countries is None:
        return False
    if country in countries:
        return True
    return False

def append_redis(key, country):
    countries = str_to_set(r.get(key))
    if not countries:
        countries = set([])
    countries.add(country)
    #print("Adding key: ", key, " - countries:", set_to_str(countries))
    return r.set(key, set_to_str(countries), expiration_in_sec)

def clean_cache(key):
    r.set(key, "")

def set_to_str(s):
    if s:
        return "_".join(s)

def str_to_set(s):
    if s:
        return set(s.split("_"))

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

list_estimates = []
valid_estimate = [] # checks if a new estimate is in between 1001 and 9999.

total_queries = df.shape[0]
if df["mau_audience"].isnull().sum() > 0:
   print("Found at least an NAN value for mau_audience...will try to find better estimates only for NAN")
   queries_need_better_estimate = df[(df["mau_audience"].isnull())].shape[0]
   TACKLE_NAN = True

   print("%d (%.3f) rows in the input file have an NaN estimation. Trying to find better estimates for those using the following countries (%s)" %
        (queries_need_better_estimate, 1.0 * queries_need_better_estimate / total_queries, countries_to_try))
else:
   queries_need_better_estimate = df[(df["mau_audience"] == 1000) ].shape[0]
   TACKLE_NAN = False
   
   print("%d (%.3f) rows in the input file have an estimated audience of 1000 people. Trying to find better estimates for those using the following countries (%s)" %
        (queries_need_better_estimate, 1.0 * queries_need_better_estimate / total_queries, countries_to_try))


# Transform str into JSON -- BAD approach. The set will be later modified and will impact in the original df. Alternatively, I could save it and get it back at the end.
#df["targeting"] = df["targeting"].apply(lambda x: x))
for country in countries_to_try:
    print ("USING COUNTRY: ", country)
    if TACKLE_NAN:
	df1000 = df[df["mau_audience"].isnull()].copy()
    else:
	df1000 = df[df["mau_audience"] == 1000].copy()
    
    df1000["exploring"] = True
    valid = pd.Series(False, index=df1000.index)

    if single_estimation:
        # In this case, we should avoid re-issueing queries that we have a valid estimate
        if len(valid_estimate) > 0:
            tmpvalid = pd.DataFrame(valid_estimate).T
            tmpvalid = tmpvalid.any(axis=1)
            # Issues only the queries that we do not have info yet
            #df1000 = df1000[~tmpvalid]
            df1000.loc[tmpvalid, "exploring"] = False

    if usingCache:
	print("Checking API calls in the cache system...")
        # if using the cache, we need the tupled version to be able to save a string in redis
        df1000["tupled"] = df1000["targeting"].apply(lambda x: str(ordered(ast.literal_eval(x))))
	df1000["tupled_country"] = df1000["targeting"].apply(lambda x: str(ordered(replace_by_country(ast.literal_eval(x), country))))

        # Only keep the rows that we have never explored
	print("Before checking cache: %d queries could have been made." % (df1000["exploring"].sum()))
        df1000.loc[df1000["tupled"].apply(lambda x: is_bad_country(x, country)), "exploring"] = False
        df1000.loc[df1000["tupled_country"].apply(lambda x: is_bad_country(x, country)), "exploring"] = False
	print("After checking cache: %d queries will be made." % (df1000["exploring"].sum()))
	print("Computing Queries...")

    df1000_in_country = df1000.copy(deep=True)
    df1000_add_country = df1000.copy(deep=True)

    # Modifies "targeting" to include country
    df1000_add_country["targeting"] = df1000_add_country["targeting"].apply(lambda x: add_country(ast.literal_eval(x), country))
    df1000_in_country["targeting"] = df1000_in_country["targeting"].apply(lambda x: replace_by_country(ast.literal_eval(x), country))

    # Check if region is inside country. If it is, return None
    df1000_add_country["targeting"] = df1000_add_country["targeting"].apply(lambda x: check_overlap(x))

    # Remove rows which targeting is invalid.
    df1000_add_country.loc[(df1000_add_country["targeting"].isnull()) | (df1000_in_country["targeting"].isnull()), "exploring"] = False
    df1000_in_country.loc[(df1000_add_country["targeting"].isnull()) | (df1000_in_country["targeting"].isnull()), "exploring"] = False

      
    try:
	# Mark all API calls that are explorable to have response = Null -> this is required by pySocialWatcher.
    	df1000_add_country.loc[df1000_add_country["exploring"], "response"] = None

	# This is the case in which we do not have any new query to issue. Just continue to the next country
	if df1000_add_country["response"].isnull().sum() == 0:
	    print("No queries to issue for this country...")
            continue

        watcherAPI.perform_collection_data_on_facebook(df1000_add_country)

	print "Second part. We should make %d API queries." % (df1000_in_country["exploring"].sum())

        # We can save API call by not exploration queries that we are invalid
        df1000_in_country.loc[ (df1000_add_country["mau_audience"] == 1000) | (df1000_add_country["mau_audience"] >= 10000), "exploring"] = False    
    	df1000_in_country.loc[df1000_in_country["exploring"], "response"] = None
	
	print "But, we are actually making %d API queries." % (df1000_in_country["exploring"].sum())
    
        # Explore the remaining queries
        watcherAPI.perform_collection_data_on_facebook(df1000_in_country)

    except Exception as err:
        # We get an error when we OR a region and a country if the region is inside the country.
        # e.g., region = doha, country = QA -> Error: Some of your locations overlap. Try removing a location.
        # We just ignore this country in our estimates
        print("Found the following error: {0}".format(err))
        print("Ignoring country %s and continuing..." % (country))
        continue

    # We first check if these estimates are good... Both datasets need to have more than 1000 and less than 10000
    v =  (df1000_in_country["mau_audience"] > 1000) & (df1000_in_country["mau_audience"] < 10000) &\
            (df1000_add_country["mau_audience"] > 1000) & (df1000_add_country["mau_audience"] < 10000) &\
            (df1000_add_country["mau_audience"] >= df1000_in_country["mau_audience"])

    # Updates the series keeping the correct indices (sync'ed with df1000)
    valid.update(v)

    valid_estimate.append(valid)

    if usingCache:
        # Informe redis that these tuples could not find good results
        bad_queries = df1000[~v]
        bad_queries["tupled"].apply(lambda x: append_redis(x, country))

	# Save in_country flag to avoid using it in the future
    	v_in_country = (df1000_in_country["mau_audience"] > 1000) & (df1000_in_country["mau_audience"] < 10000)
        bad_queries = df1000_in_country[~v_in_country]
        bad_queries["targeting"].drop_duplicates().apply(lambda x: str(ordered(x))).apply(lambda x: append_redis(x, country))

    estimates = df1000_add_country["mau_audience"] - df1000_in_country["mau_audience"]
    estimates.loc[~valid] = None
    estimates.name = country

    list_estimates.append(estimates)

    print("Finished collection for %s. Saving partial results." % (country))
    # Save the results given the current list of estimates
    result = save_partial_results(df, list_estimates, infile + "_" + country + "_")

    # For the next country, we need to change our policy to tackle None's instead of 1000's
    TACKLE_NAN = True 

# We say we got a valid estimate for a row if any auxiliary estimate is valid
valid = pd.DataFrame(valid_estimate).T
valid = valid.any(axis=1)

if result is None:
    print("ERROR: could not run for the selected countries...Exiting....")
    sys.exit(1)

if result.shape[0] != queries_need_better_estimate:
    print("WARNIG: We could not find estimators for all the regions. Try increasing the number of countries used as input. Currently using %s" % (countries_to_try))

still_can_get_better_estimates = df["mau_audience"].isnull().sum()

print("Found better estimates to %d queries." % (df[df["mau_audience"] < 1000].shape[0]))
print("Still missing to find better estimatives to %d (%.3f) queries..." % ( still_can_get_better_estimates,\
						1. * (still_can_get_better_estimates) /df.shape[0]))

save_partial_results(df, result, infile)

print("All Done.")

