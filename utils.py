import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import pandas as pd
import re


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








