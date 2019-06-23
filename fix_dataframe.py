import pandas as pd
import ast
import sys

if len(sys.argv) <= 2:
    print("fix_dataframe.py <ERROR> <FIX>")
    sys.exit(0)

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

file_with_error = sys.argv[1]
file_with_fix = sys.argv[2]

df_error = pd.read_csv(file_with_error)
df_fix = pd.read_csv(file_with_fix)

df_fix["tupled"] = df_fix["targeting"].apply(lambda x: str(ordered(ast.literal_eval(x))))
df_error["tupled"] = df_error["targeting"].apply(lambda x: str(ordered(ast.literal_eval(x))))

df_error = pd.merge(df_error, df_fix[["tupled","dau_audience","mau_audience"]], how="left", on="tupled", suffixes=("","_fixed"))

df_error["mau_audience"] = df_error["mau_audience"].where(df_error["mau_audience_fixed"].isnull(), df_error["mau_audience_fixed"])
df_error["dau_audience"] = df_error["dau_audience"].where(df_error["dau_audience_fixed"].isnull(), df_error["dau_audience_fixed"])

#del df_error["mau_audience_fixed"]
#del df_error["dau_audience_fixed"]
#del df_error["tupled"]
#del df_error["Unnamed: 0"]

df_error.to_csv(file_with_error + ".fixed")
print("Created " + file_with_error + ".fixed")
