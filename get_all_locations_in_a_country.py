#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import sys
from pysocialwatcher import watcherAPI
import string

watcher = watcherAPI()
watcher.load_credentials_file("./credentials_masoomali.csv")
watcher.config(sleep_time=0) # I am assuming that we can make requests as fast as we want

if len(sys.argv) < 3:
    print("Usage: %s <country_code> <location_type>" % (sys.argv[0]))
    print("Examples:")
    print("-- Retrieve all the cities in Brazil: %s BR city" % (sys.argv[0]))
    print("-- Retrieve all the states in Peru: %s PE region" % (sys.argv[0]))
    sys.exit(0)

country_code = sys.argv[1]
location_type = sys.argv[2]

regions = list(string.ascii_lowercase)

if location_type == "city":
    # It seems that we need a brute force approach with 2-levels to get all the cities.
    regions = [a+b for a in regions for b in regions]

def print_json(row, location_type="city", home_recent=None):
    s = ""
    if location_type == "city":
        s += '{"name": "cities", "values":[{"distance_unit":"kilometer", "key":"%d","name":"%s","region":"%s","region_id":"%d","radius":"0","country":"%s"}]'\
                    % (int(row[2]), row[3], row[4], int(row[5]), row[0] )

    elif location_type == "region":
        s += ('{"name": "regions", "values":[{"key":"%d", "country_code":"%s", "name": "%s"}]' % (int(row[2]), row[0], row[3]))

    if home_recent == "home":
        s += ', "location_type": ["home"]},'
    elif home_recent == "recent":
        s += ', "location_type": ["recent"]},'
    else:
        s += ', "location_type": ["home", "recent"]},'

    print(s)

error = []
acc = []
print("Obtaining list of locations...")
for region in regions:
    ans = watcherAPI.get_geo_locations_given_query_and_location_type(region, [location_type], country_code=country_code)
    acc.append(ans)

result = pd.concat(acc).drop_duplicates()

print("Done! Found %d locations." % (result.shape[0]))

for i in range(result.shape[0]):
    print_json(result.values[i], location_type)

