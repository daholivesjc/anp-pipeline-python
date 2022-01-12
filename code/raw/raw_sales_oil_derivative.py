from utils import ( 
    get_sheet,
    get_pivot_table_totals
)

# paths 
source = "source/vendas-combustiveis-m3.xlsx"
sink1 = "sink/raw/raw_sales_oil_deriv.parquet"
sink2 = "sink/raw/raw_sales_oil_deriv_totals.parquet"
sheet1 = "DPCache_m3"
sheet2 = "Plan1"

# get raw data of Sales of oil derivative fuels by UF and product
df_sales_oil_deriv = get_sheet(
    path=source,
    sheet=sheet1
)

# get totals of the pivot table Sales of oil derivative
df_sales_oil_deriv_pivot_table_totals = get_pivot_table_totals(
    year_position=53,
    totals_position=66,
    path=source,
    sheet=sheet2
)

# save sink
df_sales_oil_deriv.to_parquet(sink1)
df_sales_oil_deriv_pivot_table_totals.to_parquet(sink2)


