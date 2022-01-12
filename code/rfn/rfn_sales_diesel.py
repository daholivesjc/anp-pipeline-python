import pandas as pd
import re
import datetime

# paths
source = "sink/trs/trs_sales_diesel.parquet"
sink = "sink/rfn/rfn_sales_diesel.parquet"

# read dataframe parquet format
df_sales_diesel = pd.read_parquet(source)

# month dictionary
month_dict = { 
    1: "jan",
    2: "fev",
    3: "mar",
    4: "abr",
    5: "mai",
    6: "jun",
    7: "jul",
    8: "ago",
    9: "set",
    10: "out",
    11: "nov",
    12: "dez"
}

# create a new table format
list_df = []
for i,month in month_dict.items():

    df_temp = df_sales_diesel[["product","unit","uf","year",month]].copy()
    
    df_temp.rename(columns={month:"volume"}, inplace=True)
    
    list_df.append(df_temp)

# concatenate dataframes with volumes
df_new = pd.concat(list_df)

# create field year_month
df_new["year_month"] = df_new["year"].apply(lambda x: datetime.datetime(x, i, 1, 0, 0, 0).strftime('%Y-%m-%d'))

# create field timestamp
df_new["created_at"] = pd.Timestamp(datetime.datetime.now())

# drop obsolete field year
df_new.drop(columns=["year"], inplace=True)

# save sink
df_new.to_parquet(
    sink, 
    compression='snappy',
    engine='pyarrow',
    partition_cols=["year_month", "created_at"])




