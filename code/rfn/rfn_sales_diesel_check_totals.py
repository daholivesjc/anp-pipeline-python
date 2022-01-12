import pandas as pd
import numpy as np

pd.options.display.float_format = '{:.6f}'.format

# paths
source1 = "sink/rfn/rfn_sales_diesel.parquet"
source2 = "sink/trs/trs_sales_diesel_totals.parquet"

# read dataframe parquet format
df_sales_diesel = pd.read_parquet(source1)

# create year field for group by
df_sales_diesel["year"] = pd.to_datetime(
    df_sales_diesel["year_month"], 
    infer_datetime_format = True).astype('datetime64[ns]'
).dt.year
                                    
df_year_totals = df_sales_diesel.groupby(["year"]).agg({ 
    "volume": "sum"    
}).reset_index()

# origins totals from pivot table
df_sales_diesel_totals = pd.read_parquet(source2)

# volume list from data processed
list1 = df_year_totals["volume"].apply(lambda x: round(x)).values

# volume list from data totals
list2 = df_sales_diesel_totals["volume"].apply(lambda x: round(x)).values

# assert test validation
assert np.alltrue(list1 == list2)
