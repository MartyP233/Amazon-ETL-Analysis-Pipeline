import dask.dataframe as dd
import pandas as pd
import os
from dask.diagnostics import ProgressBar

ProgressBar().register()


df1 = dd.read_json("Data/amazon-sales-rank-data-for-print-and-kindle-books/ranks/ranks/000724519X_com.json", typ='series')

json_files = os.listdir("Data/ranks-norm/")

df = dd.read_json("Data/ranks-norm/*.json", orient='values', typ='series')
df = df.to_frame(name='rank')

df

df_list = []
for partition in range(df.npartitions):
    df_list.append(df.get_partition(partition).assign(asin=json_files[partition]))

df

with ProgressBar():
    dfs = dd.concat(df_list)

with ProgressBar():
    dfsa = dfs.compute()

ddjson = {}
for filename in json_files:
    ddjson[filename] = pd.read_json(f"Data/ranks-norm/{filename}", orient='values', typ='series')

# 1 method - pandas

dfs = [pd.read_json(os.path.join("Data/ranks-norm", f), typ='series').to_frame(name='rank').assign(ASIN=f) for f in json_files]
df = pd.concat(dfs, ignore_index=True)



