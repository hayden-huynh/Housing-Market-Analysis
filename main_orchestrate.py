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
    # 'queue': 'bash_queue',
    # "pool": "default_pool",
    "priority_weight": 1,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
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
    # wait_for_car_dag = ExternalTaskSensor(
    #     task_id="wait_for_car_dag",
    #     external_dag_id="cargurus_used_cars",  # ID of DAG 1
    #     external_task_id="load_data",  # Wait for this specific task in dag_1
    #     mode="poke",  # Continuously check for success
    #     timeout=600,  # Timeout in seconds if dag_1 doesn't complete
    #     poke_interval=5,  # Check every 5 seconds
    #     allowed_states=["success"],
    #     failed_states=["failed"],
    # )

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
