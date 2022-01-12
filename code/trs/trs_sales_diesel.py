import pandas as pd
import re

# paths
source = "sink/raw/raw_sales_diesel.parquet"
sink = "sink/trs/trs_sales_diesel.parquet"

# read dataframe parquet format
df_sales_diesel = pd.read_parquet(source)

# dictionary with new columns
columns = {
    "COMBUSTÍVEL": "product",
    "ANO": "year",
    "REGIÃO": "region",
    "ESTADO": "uf",
    "Jan": "jan",
    "Fev": "fev",
    "Mar": "mar",
    "Abr": "abr",
    "Mai": "mai",
    "Jun": "jun",
    "Jul": "jul",
    "Ago": "ago",
    "Set": "set",
    "Out": "out",
    "Nov": "nov",
    "Dez": "dez",
    "TOTAL": "total"
}

# rename columns
df_sales_diesel = df_sales_diesel.rename(columns=columns)

# create unit field
df_sales_diesel["unit"] = df_sales_diesel["product"].apply(lambda x: x[-3:-1])

# adjust product field
df_sales_diesel["product"] = df_sales_diesel["product"].apply(lambda x: x[0:-4].strip())

# order columns
df_sales_diesel = df_sales_diesel[
    ["product","unit","region","uf","year","jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez","total"]
]

# save sink
df_sales_diesel.to_parquet(sink)