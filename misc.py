import pandas as pd
import numpy as np
import datetime

def display(value):
    s = str(int(value))
    if len(s) == 4:
        return s[0] + "." + s[1] + "K"
    elif len(s) >=5 and len(s)  <= 6:
        return s[0:len(s) - 3] + "K"
    elif len(s) == 7:
        return s[0] + "." + s[1] + "M"
    elif len(s) >=8 and len(s)  <= 9:
        return s[0:len(s) - 6] + "M"
    elif len(s) == 10:
        return s[0] + "." + s[1] + "B"
    elif len(s) >=11 and len(s)  <= 12:
        return s[0:len(s) - 9] + "B"
    return s

def convert_unixtime(utime):
    return (datetime.datetime.fromtimestamp(int(utime)).strftime('%d-%m-%y'))

def convert_country_code(code):
    mapping = {
            "AD": "Andorra",
            "AE": "United Arab Emirates",
            "AF": "Afghanistan",
            "AG": "Antigua and Barbuda",
            "AL": "Albania",
            "AM": "Armenia",
            "AO": "Angola",
            "AR": "Argentina",
            "AS": "American Samoa",
            "AT": "Austria",
            "AU": "Australia",
            "AW": "Aruba",
            "AZ": "Azerbaijan",
            "BA": "Bosnia and Herzegovina",
            "BB": "Barbados",
            "BD": "Bangladesh",
            "BE": "Belgium",
            "BF": "Burkina Faso",
            "BG": "Bulgaria",
            "BH": "Bahrain",
            "BI": "Burundi",
            "BJ": "Benin",
            "BM": "Bermuda",
            "BN": "Brunei",
            "BO": "Bolivia",
            "BR": "Brazil",
            "BS": "Bahamas",
            "BT": "Bhutan",
            "BW": "Botswana",
            "BY": "Belarus",
            "BZ": "Belize",
            "CA": "Canada",
            "CD": "Congo Dem. Rep.",
            "CF": "Central African Republic",
            "CG": "Congo Rep.",
            "CH": "Switzerland",
            "CI": "Cote d'Ivoire",
            "CK": "Cook Islands",
            "CL": "Chile",
            "CM": "Cameroon",
            "CN": "China",
            "CO": "Colombia",
            "CR": "Costa Rica",
            "CV": "Cape Verde",
            "CW": "Curacao",
            "CY": "Cyprus",
            "CZ": "Czech Republic",
            "DE": "Germany",
            "DJ": "Djibouti",
            "DK": "Denmark",
            "DM": "Dominica",
            "DO": "Dominican Republic",
            "DZ": "Algeria",
            "EC": "Ecuador",
            "EE": "Estonia",
            "EG": "Egypt",
            "EH": "Western Sahara",
            "ER": "Eritrea",
            "ES": "Spain",
            "ET": "Ethiopia",
            "FI": "Finland",
            "FJ": "Fiji",
            "FK": "Falkland Islands",
            "FM": "Micronesia",
            "FO": "Faroe Islands",
            "FR": "France",
            "GA": "Gabon",
            "GB": "United Kingdom",
            "GD": "Grenada",
            "GE": "Georgia",
            "GF": "French Guiana",
            "GG": "Guernsey",
            "GH": "Ghana",
            "GI": "Gibraltar",
            "GL": "Greenland",
            "GM": "Gambia",
            "GN": "Guinea-Bissau",
            "GP": "Guadeloupe",
            "GQ": "Equatorial Guinea",
            "GR": "Greece",
            "GT": "Guatemala",
            "GU": "Guam",
            "GW": "Guinea",
            "GY": "Guyana",
            "HK": "Hong Kong",
            "HN": "Honduras",
            "HR": "Croatia",
            "HT": "Haiti",
            "HU": "Hungary",
            "ID": "Indonesia",
            "IE": "Ireland",
            "IL": "Israel",
            "IM": "Isle of Man",
            "IN": "India",
            "IQ": "Iraq",
            "IS": "Iceland",
            "IT": "Italy",
            "JE": "Jersey",
            "JM": "Jamaica",
            "JO": "Jordan",
            "JP": "Japan",
            "KE": "Kenya",
            "KG": "Kyrgyzstan",
            "KH": "Cambodia",
            "KI": "Kiribati",
            "KM": "Comoros",
            "KN": "Saint Kitts and Nevis",
            "KR": "South Korea",
            "KW": "Kuwait",
            "KY": "Cayman Islands",
            "KZ": "Kazakhstan",
            "LA": "Laos",
            "LB": "Lebanon",
            "LC": "Saint Lucia",
            "LI": "Liechtenstein",
            "LK": "Sri Lanka",
            "LR": "Liberia",
            "LS": "Lesotho",
            "LT": "Lithuania",
            "LU": "Luxembourg",
            "LV": "Latvia",
            "LY": "Libya",
            "MA": "Morocco",
            "MC": "Monaco",
            "MD": "Moldova",
            "ME": "Montenegro",
            "MF": "Saint Martin",
            "MG": "Madagascar",
            "MH": "Marshall Islands",
            "MK": "Macedonia",
            "ML": "Mali",
            "MM": "Myanmar",
            "MN": "Mongolia",
            "MO": "Macau",
            "MP": "Northern Mariana Islands",
            "MQ": "Martinique",
            "MR": "Mauritania",
            "MS": "Montserrat",
            "MT": "Malta",
            "MU": "Mauritius",
            "MV": "Maldives",
            "MW": "Malawi",
            "MX": "Mexico",
            "MY": "Malaysia",
            "MZ": "Mozambique",
            "NA": "Namibia",
            "NC": "New Caledonia",
            "NE": "Niger",
            "NF": "Norfolk Island",
            "NG": "Nigeria",
            "NI": "Nicaragua",
            "NL": "Netherlands",
            "NO": "Norway",
            "NP": "Nepal",
            "NR": "Nauru",
            "NU": "Niue",
            "NZ": "New Zealand",
            "OM": "Oman",
            "PA": "Panama",
            "PE": "Peru",
            "PF": "French Polynesia",
            "PG": "Papua New Guinea",
            "PH": "Philippines",
            "PK": "Pakistan",
            "PL": "Poland",
            "PM": "Saint Pierre and Miquelon",
            "PN": "Pitcairn",
            "PR": "Puerto Rico",
            "PS": "Palestine",
            "PT": "Portugal",
            "PW": "Palau",
            "PY": "Paraguay",
            "QA": "Qatar",
            "RE": "Reunion",
            "RO": "Romania",
            "RS": "Serbia",
            "RU": "Russia",
            "RW": "Rwanda",
            "SA": "Saudi Arabia",
            "SB": "Solomon Islands",
            "SC": "Seychelles",
            "SE": "Sweden",
            "SG": "Singapore",
            "SH": "Saint Helena",
            "SI": "Slovenia",
            "SJ": "Svalbard and Jan Mayen",
            "SK": "Slovakia",
            "SL": "Sierra Leone",
            "SM": "San Marino",
            "SN": "Senegal",
            "SO": "Somalia",
            "SR": "Suriname",
            "SS": "South Sudan",
            "ST": "Sao Tome and Principe",
            "SV": "El Salvador",
            "SX": "Sint Maarten",
            "SZ": "Swaziland",
            "TC": "Turks and Caicos Islands",
            "TD": "Chad",
            "TG": "Togo",
            "TH": "Thailand",
            "TJ": "Tajikistan",
            "TK": "Tokelau",
            "TL": "Timor-Leste",
            "TM": "Turkmenistan",
            "TN": "Tunisia",
            "TO": "Tonga",
            "TR": "Turkey",
            "TT": "Trinidad and Tobago",
            "TV": "Tuvalu",
            "TW": "Taiwan",
            "TZ": "Tanzania",
            "UA": "Ukraine",
            "UG": "Uganda",
            "US": "United States",
            "UY": "Uruguay",
            "UZ": "Uzbekistan",
            "VC": "Saint Vincent and the Grenadines",
            "VE": "Venezuela",
            "VG": "British Virgin Islands",
            "VI": "US Virgin Islands",
            "VN": "Vietnam",
            "VU": "Vanuatu",
            "WF": "Wallis and Futuna",
            "WS": "Samoa",
            "XK": "Kosovo",
            "YE": "Yemen",
            "YT": "Mayotte",
            "ZA": "South Africa",
            "ZM": "Zambia",
            "ZW": "Zimbabwe",
 }
    return mapping[code]

def cut(df, col, values, savedcols= ['Location', 'dau_audience', 'mau_audience']):
    df_slice = {}

    for i, v in enumerate(values):
        df_slice[i] = df[df[col] == v][savedcols]

    merge = pd.merge(df_slice[0], df_slice[1], on=["Location"], suffixes=("_%s" % (values[0]), "_%s" % (values[1])))
    for i in range(2, len(values)):
        #print("Adding suffix for:" + values[i])
        merge = pd.merge(merge, df_slice[i], on=["Location"], suffixes=("", "_%s" % (values[i])))

    if len(values) > 2:
        # Fix information for the second group.
        merge["audience_%s" % (values[2])] = merge["audience"]
        del merge["audience"]
    return merge

def copy_rename(df, oldname, newname):
    return df.rename(columns={oldname: newname}).copy()

def get_slice(dfin, col, values, frequency="mau"):
    dfout = copy_rename(dfin, '%s_audience' % (frequency), "audience")
    dfout = cut(dfout, col, values, savedcols=['Location', 'audience'])
    dfout["Frequency"] = "Daily" if frequency == "dau" else "Monthly"
    return dfout

def calculate_percentages(df, cols, prefix):
    sumcols = ["audience_" + col for col in cols]

    totals = df[sumcols].sum(axis=1)
    for col in cols:
        df[prefix + "audience_" + col] = df["audience_" + col] * 100. / totals
        df[prefix + "audience_" + col] = df["audience_" + col] * 100. / totals


def extract_relationship(d):
    if 1 in d:
        return "single"
    elif 2 in d:
        return "dating"
    elif 3 in d:
        return "married"
    else:
        return None

def extract_education(d):
    if d == [3, 7, 8, 9, 11]:
        return "graduated"
    elif d == [1, 12, 13]:
        return "no_degree"
    elif d == [2, 4, 5, 6, 10]:
        return "high_school"
    elif d == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]:
        return "all"
    else:
        return None

def extract_group(d):
    if type(d) == dict:
        id = d["id"]
    else:
        id = d[0]["id"]

    if id == 6026404871583:
        return "Expats (Venezuela)"
    elif id == 6015559470583:
        return "Ex-pats (All)"
    else:
        return "All"

def agebuckets(minage, maxage):
    if minage == 13 and (maxage is None or np.isnan(maxage)):
        return "all"
    elif minage == 13 and maxage == 18:
        return "adolecent"
    elif minage == 19 and maxage == 25:
        return "young_adult"
    elif minage == 26 and maxage == 40:
        return "adult"
    elif minage == 41 and maxage == 65:
        return "middle_age"
    elif minage == 65 and (maxage is None or np.isnan(maxage)):
        return "elder"
    return "undefined"

def expand(row):
    place = None
    loc_dimension = None
    if "regions" in row["geo_locations"]:
        region = row["geo_locations"]["regions"][0]["name"]
        country = row["geo_locations"]["regions"][0]["country_code"] if "country_code" in row["geo_locations"]["regions"][0] else row["geo_locations"]["regions"][0]["country"]
        country = convert_country_code(country)
        place = "%s, %s" % (region, country)
        loc_dimension = "State"
    elif "countries" in row["geo_locations"]:
        place = convert_country_code(row["geo_locations"]["countries"][0])
        loc_dimension = "Country"
    elif "cities" in row["geo_locations"]:
        place = "%s, %s, %s" % (row["geo_locations"]["cities"][0]["name"], row["geo_locations"]["cities"][0]["region"], convert_country_code(row["geo_locations"]["cities"][0]["country"]))
        loc_dimension = "City"
    elif "custom_locations" in row["geo_locations"]:
        #place = "%.4f,%.4f,%d" % (row["geo_locations"]["custom_locations"][0]["latitude"],
        #                      row["geo_locations"]["custom_locations"][0]["longitude"],
        #                      row["geo_locations"]["custom_locations"][0]["radius"])
        place = "%s" % (row["geo_locations"]["custom_locations"][0]["name"])
        loc_dimension = "Coordinates"

    loctype = "_".join(row["geo_locations"]["location_types"])

    relationship, education, group = None, None, None
    for dimension in row["flexible_spec"]:
        if "relationship_statuses" in dimension:
            relationship = extract_relationship(dimension["relationship_statuses"])
        elif "education_statuses" in dimension:
            education = extract_education(dimension["education_statuses"])
        elif "behaviors" in dimension:
            group = extract_group(dimension["behaviors"])

    gender = row["genders"][0]
    gender = "both" if gender == 0 else "man" if gender == 1 else "woman"

    return row["age_min"], row["age_max"], place, loc_dimension, loctype, gender, relationship, education, group

def get_item(x):
    if not x:
        return None
    return x["name"]

