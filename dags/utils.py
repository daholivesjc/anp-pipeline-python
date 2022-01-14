import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import pandas as pd
import re
import datetime
import numpy as np
import shutil
import os


def get_workbook(path):
    
    wb = load_workbook(path)
    
    return wb


def get_sheet(path, sheet):
    
    workbook = get_workbook(path)
    
    ws = workbook[sheet]

    data = ws.values 

    columns = next(data)[0:]

    df = pd.DataFrame(data, columns=columns)
    
    return df


def get_pivot_table_totals(year_position, totals_position, path, sheet):
    
    regnumber = re.compile(r'\d+(?:,\d*)?')
    
    workbook = get_workbook(path)
    ws_plan1 = workbook[sheet]

    row_year = ws_plan1[year_position]
    row_total = ws_plan1[totals_position]

    df = pd.DataFrame(
        [(int(year.value), total.value) for (year,total) in zip(row_year, row_total) if regnumber.match(str(year.value))],
        columns=["year","total"]
    )

    return df


#################
# raw functions #
#################
def run_raw_diesel():

    # paths
    source = "/opt/airflow/dags/source/vendas-combustiveis-m3.xlsx"
    sink1 = "/opt/airflow/dags/sink/raw/raw_sales_diesel.parquet"
    sink2 = "/opt/airflow/dags/sink/raw/raw_sales_diesel_totals.parquet"
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

    return True


def run_raw_sales_oil_derivative():

    # paths 
    source = "/opt/airflow/dags/source/vendas-combustiveis-m3.xlsx"
    sink1 = "/opt/airflow/dags/sink/raw/raw_sales_oil_deriv.parquet"
    sink2 = "/opt/airflow/dags/sink/raw/raw_sales_oil_deriv_totals.parquet"
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

    return True


#####################
# trusted functions #
#####################
def run_trs_sales_diesel():

    # paths
    source = "/opt/airflow/dags/sink/raw/raw_sales_diesel.parquet"
    sink = "/opt/airflow/dags/sink/trs/trs_sales_diesel.parquet"

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

    return True


def run_trs_sales_oil_derivative():

    # paths
    source = "/opt/airflow/dags/sink/raw/raw_sales_oil_deriv.parquet"
    sink = "/opt/airflow/dags/sink/trs/trs_sales_oil_deriv.parquet"

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

    return True


def run_trs_sales_diesel_totals():

    # paths
    source = "/opt/airflow/dags/sink/raw/raw_sales_diesel_totals.parquet"
    sink = "/opt/airflow/dags/sink/trs/trs_sales_diesel_totals.parquet"

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

    return True


def run_trs_sales_oil_derivative_totals():

    # paths
    source = "/opt/airflow/dags/sink/raw/raw_sales_oil_deriv_totals.parquet"
    sink = "/opt/airflow/dags/sink/trs/trs_sales_oil_deriv_totals.parquet"

    # read dataframe parquet format
    df_sales_oil_deriv_totals = pd.read_parquet(source)

    # dictionary with new columns
    columns = {
        "total": "volume"
    }

    # rename columns
    df_sales_oil_deriv_totals = df_sales_oil_deriv_totals.rename(columns=columns)

    # save sink
    df_sales_oil_deriv_totals.to_parquet(sink)

    return True


#####################
# refined functions #
#####################
def run_rfn_sales_diesel():

    # paths
    source = "/opt/airflow/dags/sink/trs/trs_sales_diesel.parquet"
    sink = "/opt/airflow/dags/sink/rfn/rfn_sales_diesel.parquet"

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

    # remove last parquet
    if os.path.exists(sink):
        shutil.rmtree(sink)

    # save sink
    df_new.to_parquet(
        sink, 
        compression='snappy',
        engine='pyarrow',
        partition_cols=["year_month", "created_at"])

    return True

def run_rfn_sales_oil_derivative():

    # paths
    source = "/opt/airflow/dags/sink/trs/trs_sales_oil_deriv.parquet"
    sink = "/opt/airflow/dags/sink/rfn/rfn_sales_oil_deriv.parquet"

    # read dataframe parquet format
    df_sales_oil_deriv = pd.read_parquet(source)

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

        df_temp = df_sales_oil_deriv[["product","unit","uf","year",month]].copy()
        
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

    # remove last parquet
    if os.path.exists(sink):
        shutil.rmtree(sink)

    # save sink
    df_new.to_parquet(
        sink, 
        compression='snappy',
        engine='pyarrow',
        partition_cols=["year_month", "created_at"])

    return True


def run_rfn_sales_diesel_check_totals():

    pd.options.display.float_format = '{:.6f}'.format

    # paths
    source1 = "/opt/airflow/dags/sink/rfn/rfn_sales_diesel.parquet"
    source2 = "/opt/airflow/dags/sink/trs/trs_sales_diesel_totals.parquet"

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

    return True


def run_rfn_sales_oil_derivative_check_totals():

    pd.options.display.float_format = '{:.6f}'.format

    # paths
    source1 = "/opt/airflow/dags/sink/rfn/rfn_sales_oil_deriv.parquet"
    source2 = "/opt/airflow/dags/sink/trs/trs_sales_oil_deriv_totals.parquet"

    # read dataframe parquet format
    df_sales_oil_deriv = pd.read_parquet(source1)

    # create year field for group by
    df_sales_oil_deriv["year"] = pd.to_datetime(
        df_sales_oil_deriv["year_month"], 
        infer_datetime_format = True).astype('datetime64[ns]'
    ).dt.year
                                        
    df_year_totals = df_sales_oil_deriv.groupby(["year"]).agg({ 
        "volume": "sum"    
    }).reset_index()

    # origins totals from pivot table
    df_sales_oil_deriv_totals = pd.read_parquet(source2)

    # volume list from data processed
    list1 = df_year_totals["volume"].apply(lambda x: round(x)).values

    # volume list from data totals
    list2 = df_sales_oil_deriv_totals["volume"].apply(lambda x: round(x)).values

    # assert test validation
    assert np.alltrue(list1 == list2)   

    return True