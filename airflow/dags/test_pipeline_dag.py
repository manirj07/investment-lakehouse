from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.task_runner import run_python_script


with DAG(
    dag_id="test_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    market_prices = PythonOperator(
        task_id="market_prices_ingestion",
        python_callable=run_python_script,
        op_kwargs={
            "script": "src/bronze/market_prices_ingestion.py"
        },
    )