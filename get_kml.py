import io
from pysocialwatcher import watcherAPI
import xml.etree.ElementTree as ET

import json
import pandas as pd

watcher = watcherAPI()
watcher.load_credentials_file("./credentials_masoomali.csv")

#watcherAPI.get_kml_given_geolocation("cities", [244598])
# All regions in Peru
# dfpe = watcherAPI.get_kml_given_geolocation("regions", [2760, 2757, 2758, 2761, 2759, 2763, 2762, 2764, 2773, 2766, 2765, 2768, 2771, 2769, 2772, 2770, 2774, 2778, 2781, 2780, 2767, 2775, 2776, 2779, 2777])
#
#dfpa = watcherAPI.get_kml_given_geolocation("regions", [2747, 2755, 4195, 2750, 2749, 2748, 2751, 4199, 4200, 4196, 2752, 2753, 2754, 2756])

country = None
def df_to_folium(row):
    out = {}

    out["type"] = "Feature"
    if country is not None:
        out["id"] = row["name"] + ", " + country
    else:
        out["id"] = row["name"]
    out["properties"] = {"name" : row["name"]}
    skml = row["kml"]

    xml_kml = ET.fromstring("<root>" + skml + "</root>")
    coordinates = xml_kml.findall(".//coordinates")

    list_of_coords = []
    for c in coordinates:
        s = c.text
        coor = []
        for pair in s.split():
            a,b = map(float,pair.split(","))
            coor.append([a,b])
        list_of_coords.append(coor)

    polygon = {"type":"Polygon", "coordinates":list_of_coords}
    out["geometry"] = polygon
    return out

"""
df = watcherAPI.get_kml_given_geolocation("regions", [438, 439, 441, 440, 442, 443, 459, 454, 455, 456, 446, 444, 445, 462, 449, 448, 447, 463, 461, 458, 464, 457, 460, 452, 453, 451, 450])
features = list(df.apply(df_to_folium, axis=1))
output = {"type":"FeatureCollection","features":features}
with io.open("states_%s.json" % (country), "w", encoding="utf-8") as f:
    #json.dump(output, f)
    json_string = json.dumps(output, ensure_ascii=False)
    f.write(unicode(json_string))
"""

#df = watcherAPI.get_kml_given_geolocation("countries", ["BR", "CO", "EC", "CL", "AR", "UY", "PE", "PA"])
#outname = "countries.json"

df = watcherAPI.get_kml_given_geolocation("regions", [438, 439, 441, 440, 442, 443, 459, 454, 455, 456, 446, 444, 445, 462, 449, 448, 447, 463, 461, 458, 464, 457, 460, 452, 453, 451, 450])
outname = "states_brazil.json"

features = list(df.apply(df_to_folium, axis=1))
output = {"type":"FeatureCollection","features":features}
with io.open(outname, "w", encoding="utf-8") as f:
    json_string = json.dumps(output, ensure_ascii=False)
    f.write(unicode(json_string))




