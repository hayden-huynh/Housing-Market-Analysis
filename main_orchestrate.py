from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys

sys.path.insert(0, "/home/hayden_huynh/Projects/Housing-Market-Analysis")
from main_extract import run_main_extract
from main_load import main_load

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
    "priority_weight": 1,
}

with DAG(
    "trulia_housing_market",
    default_args=default_args,
    description="ELT pipeline for housing properties listed on Trulia",
    schedule="0 14 * * *",
    start_date=datetime(2025, 11, 1, 0, 0),
    catchup=False,
    tags=["ELT", "Trulia", "Housing Properties"],
) as dag:
    hma_extract_data = BashOperator(
        task_id="hma_extract_data",
        bash_command="cd /home/hayden_huynh/Projects/Housing-Market-Analysis/ && source ./venv/bin/activate && python main_extract.py",
    )

    hma_load_data = BashOperator(
        task_id="hma_load_data",
        bash_command="cd /home/hayden_huynh/Projects/Housing-Market-Analysis/ && source ./venv/bin/activate && python main_load.py",
    )

    hma_transform_data = BashOperator(
        task_id="hma_transform_data",
        bash_command="cd /home/hayden_huynh/Projects/Housing-Market-Analysis/dbt_HMA/ && source ../venv/bin/activate && python -m dotenv run dbt run",
    )

    hma_extract_data >> hma_load_data >> hma_transform_data
