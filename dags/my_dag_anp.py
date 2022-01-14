from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

# raw
from utils import run_raw_diesel
from utils import run_raw_sales_oil_derivative
from utils import run_trs_sales_diesel
from utils import run_trs_sales_oil_derivative
from utils import run_trs_sales_diesel_totals
from utils import run_trs_sales_oil_derivative_totals
from utils import run_rfn_sales_diesel
from utils import run_rfn_sales_oil_derivative
from utils import run_rfn_sales_diesel_check_totals
from utils import run_rfn_sales_oil_derivative_check_totals


with DAG("my_dag_anp", start_date=datetime(2022,1,12), schedule_interval="@daily", catchup=False) as dag:

    start = DummyOperator(task_id='start')

    # Raw zone
    raw = DummyOperator(task_id='raw')

    raw_diesel = PythonOperator(
        task_id="raw_diesel",
        python_callable=run_raw_diesel
    )

    raw_sales_oil_derivative = PythonOperator(
        task_id="raw_sales_oil_derivative",
        python_callable=run_raw_sales_oil_derivative
    )

    # Trusted zone
    trusted = DummyOperator(task_id='trusted')

    start >> raw >> [raw_diesel,raw_sales_oil_derivative] >> trusted

    trs_sales_diesel = PythonOperator(
        task_id="trs_sales_diesel",
        python_callable=run_trs_sales_diesel
    )

    trs_sales_oil_derivative = PythonOperator(
        task_id="trs_sales_oil_derivative",
        python_callable=run_trs_sales_oil_derivative
    )

    trs_sales_diesel_totals = PythonOperator(
        task_id="trs_sales_diesel_totals",
        python_callable=run_trs_sales_diesel_totals
    )

    trs_sales_oil_derivative_totals = PythonOperator(
        task_id="trs_sales_oil_derivative_totals",
        python_callable=run_trs_sales_oil_derivative_totals
    )

    # Refined zone
    refined = DummyOperator(task_id='refined')

    trusted >> [trs_sales_diesel, trs_sales_oil_derivative, trs_sales_diesel_totals, trs_sales_oil_derivative_totals] >> refined

    rfn_sales_diesel = PythonOperator(
        task_id="rfn_sales_diesel",
        python_callable=run_rfn_sales_diesel
    )

    rfn_sales_oil_derivative = PythonOperator(
        task_id="rfn_sales_oil_derivative",
        python_callable=run_rfn_sales_oil_derivative
    )

    rfn_sales_diesel_check_totals = PythonOperator(
        task_id="rfn_sales_diesel_check_totals",
        python_callable=run_rfn_sales_diesel_check_totals
    )

    rfn_sales_oil_derivative_check_totals = PythonOperator(
        task_id="rfn_sales_oil_derivative_check_totals",
        python_callable=run_rfn_sales_oil_derivative_check_totals
    )

    refined >> [rfn_sales_diesel,rfn_sales_oil_derivative]

    rfn_sales_diesel >> rfn_sales_diesel_check_totals

    rfn_sales_oil_derivative >> rfn_sales_oil_derivative_check_totals

    end = DummyOperator(task_id='end')

    [rfn_sales_diesel_check_totals,rfn_sales_oil_derivative_check_totals] >> end