from utils import ( 
    get_sheet,
    get_pivot_table_totals
)

# paths
source = "source/vendas-combustiveis-m3.xlsx"
sink1 = "sink/raw/raw_sales_diesel.parquet"
sink2 = "sink/raw/raw_sales_diesel_totals.parquet"
sheet1 = "DPCache_m3_2"
sheet2 = "Plan1"

# get raw data of Sales of diesel by UF and type
df_sales_diesel = get_sheet(
    path=source,
    sheet=sheet1
)

# get totals of the pivot table Sales of diesel by UF and type
df_sales_diesel_totals = get_pivot_table_totals(
    year_position=133,
    totals_position=146,
    path=source,
    sheet=sheet2
)

# save sink
df_sales_diesel.to_parquet(sink1)
df_sales_diesel_totals.to_parquet(sink2)

