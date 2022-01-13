import pandas as pd
import re

# paths
source = "sink/raw/raw_sales_diesel_totals.parquet"
sink = "sink/trs/trs_sales_diesel_totals.parquet"

# read dataframe parquet format
df_sales_diesel_totals = pd.read_parquet(source)

# dictionary with new columns
columns = {
    "total": "volume"
}

# rename columns
df_sales_diesel_totals = df_sales_diesel_totals.rename(columns=columns)

# save sink
df_sales_diesel_totals.to_parquet(sink)


