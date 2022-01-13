import pandas as pd
import re

# paths
source = "sink/raw/raw_sales_oil_deriv.parquet"
sink = "sink/trs/trs_sales_oil_deriv.parquet"

# read dataframe parquet format
df_sales_oil_deriv = pd.read_parquet(source)

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
df_sales_oil_deriv = df_sales_oil_deriv.rename(columns=columns)

# create unit field
df_sales_oil_deriv["unit"] = df_sales_oil_deriv["product"].apply(lambda x: x[-3:-1])

# adjust product field
df_sales_oil_deriv["product"] = df_sales_oil_deriv["product"].apply(lambda x: x[0:-4].strip())

# order columns
df_sales_oil_deriv = df_sales_oil_deriv[
    ["product","unit","region","uf","year","jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez","total"]
]

# save sink
df_sales_oil_deriv.to_parquet(sink)





