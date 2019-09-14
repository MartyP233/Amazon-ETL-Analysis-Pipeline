import dask.dataframe as dd
import pandas as pd
import os
from dask.diagnostics import ProgressBar

ProgressBar().register()


df1 = dd.read_json("Data/amazon-sales-rank-data-for-print-and-kindle-books/ranks/ranks/000724519X_com.json", typ='series')

json_files = os.listdir("Data/ranks_norm/")
asins = list(map(lambda each:each.strip("_com_norm.json"), json_files))

df = dd.read_json("Data/ranks_norm/*.json", orient='values', typ='series')
df = df.to_frame(name='rank')

df

# opt 1: append to list approach

df_list = []
for partition in range(df.npartitions):
    df_list.append(df.get_partition(partition).assign(asin=json_files[partition]))

df

with ProgressBar():
    dfs = dd.concat(df_list)

with ProgressBar():
    dfsa = dfs.compute()

# opt 2: map partitions approach

def add_filename(df, filename):
        return df.assign(asin=filename)
# apply's one name to every partition..
dfwf_test = df.map_partitions(add_filename, json_files[0])



with ProgressBar():
    dfsa = dfwf.compute()


# alt approach

ddjson = {}
for filename in json_files:
    ddjson[filename] = pd.read_json(f"Data/ranks-norm/{filename}", orient='values', typ='series')

# 1 method - pandas

dfs = [pd.read_json(os.path.join("Data/ranks-norm", f), typ='series').to_frame(name='rank').assign(ASIN=f) for f in json_files]
df = pd.concat(dfs, ignore_index=True)

# skip pandas

import json
import ast

#dict = json.loads("Data/ranks-norm/000721393X_com_norm.json")
dict = json.load(open("Data/ranks-norm/000721393X_com_norm.json"))

# write file back out

#dfs = [pd.read_json(os.path.join("Data/ranks-norm", f), typ='series').to_frame(name='rank').assign(ASIN=f).to_json(f"processed_{f}") for f in json_files]
