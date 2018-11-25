# Code to check for errors given a new df and an old one.
import json
import pandas as pd
import numpy as np
import ast
import sys

if len(sys.argv) <= 2:
    print("check_errors.py <OLD> <NEW>")
    sys.exit(0)

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

file_old = sys.argv[1]
file_new = sys.argv[2]

df_new = pd.read_csv(file_new)
df_old = pd.read_csv(file_old)

df_new["tupled"] = df_fix["targeting"].apply(lambda x: str(ordered(ast.literal_eval(x))))
df_old["tupled"] = df_error["targeting"].apply(lambda x: str(ordered(ast.literal_eval(x))))

merged = pd.merge(df_new[["mau_audience","dau_audience","tupled"]], df_old[["mau_audience","dau_audience","tupled"]], on=[u'tupled'], suffixes=("_new", "_old"))

withproblem = merged[((np.abs(merged["mau_audience_new"] - merged["mau_audience_old"]) > 2000) & ((merged["mau_audience_new"] <= 1000) & (merged["mau_audience_old"] >= 1000)))
                    | ((merged["mau_audience_new"] <= merged["dau_audience_new"]) & (merged["mau_audience_new"] <= 1000))
                    | ((merged["dau_audience_new"] - merged["dau_audience_old"] > 1000) & (merged["dau_audience_new"] <= 1000)) ]["tupled"].reset_index()

reissue = pd.merge(df_new, withproblem, on =["tupled"])

del reissue["index"]
del reissue["tupled"]
del reissue["Unnamed: 0"]
reissue["response"] = None

reissue.to_csv("reissue.csv")
print("Needs to re-issue %d api calls. " % (reissue.shape[0]))

