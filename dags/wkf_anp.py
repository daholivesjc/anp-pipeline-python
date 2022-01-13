
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

# raw
import raw.raw_sales_oil_derivative as raw_sales_oil_derivative
import raw.raw_sales_diesel as raw_sales_diesel

# trusted
import code.trs.trs_sales_oil_derivative as trs_sales_oil_derivative
import code.trs.trs_sales_oil_derivative_totals as trs_sales_oil_derivative_totals
import code.trs.trs_sales_diesel as trs_sales_diesel
import code.trs.trs_sales_oil_derivtrs_sales_diesel_totalsative as trs_sales_oil_derivtrs_sales_diesel_totalsative

# refined
import code.rfn.rfn_sales_oil_derivative as rfn_sales_oil_derivative
import code.rfn.rfn_sales_oil_derivative_check_totals as rfn_sales_oil_derivative_check_totals
import code.rfn.rfn_sales_diesel as rfn_sales_diesel
import code.rfn.rfn_sales_diesel_check_totals as rfn_sales_diesel_check_totals


with DAG("anp_pipeline", 
    start_date=datetime(2022,1,12), 
    schedule_interval="@daily", 
    catchup=False) as dag:

    start = DummyOperator(task_id='start')

    # Raw zone
    raw = DummyOperator(task_id='raw')

    start >> raw

    raw >> [raw_sales_oil_derivative,raw_sales_diesel]