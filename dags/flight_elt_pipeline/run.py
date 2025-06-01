from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helper.logger import logger
from flight_elt_pipeline.tasks.staging.extract_to_minio import extract
from flight_elt_pipeline.tasks.staging.load_to_staging import load
from flight_elt_pipeline.tasks.warehouse.transform_to_dwh import transform
from airflow.models import Variable
from helper.callbacks.slack_notifier import slack_notifier
from pendulum import datetime

# Initialize the logger instance for this DAG
# logger = logger(logger_name="main-dag-log")

# Initialize slack notifier for alerting/callback if there is any error
default_args = {
    'on_failure_callback' : slack_notifier
}

@dag(
    dag_id='flight_elt_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args
)

def flight_elt_pipeline():
    """
    Main DAG function for the travel ELT pipeline.

    This DAG orchestrates the ETL pipeline composed of three phases:
    1. Extract: Extract data from source DB and save to a storage bucket.
    2. Load: Load extracted data into staging schema in the warehouse.
    3. Transform: Run transformations and load data into final DWH schema.

    Uses Airflow task groups for logical grouping of each phase's tasks,
    and chains them sequentially with clear start and end markers.
    """

    incremental_mode = eval(Variable.get('incremental'))

    extract_tasks = extract(incremental=incremental_mode)
    load_first_task, load_last_task= load(incremental=incremental_mode)
    transform_first_task, transform_last_task = transform()

    start = EmptyOperator(task_id="start")
    extract_done = EmptyOperator(task_id="extract_done")
    load_done = EmptyOperator(task_id="load_done")
    transform_done = EmptyOperator(task_id="transform_done")
    end = EmptyOperator(task_id="end")

    start >> extract_tasks
    for task in extract_tasks:
        task >> extract_done
    extract_done >> load_first_task
    load_last_task >> load_done
    load_done >> transform_first_task
    transform_last_task >> transform_done
    transform_done >> end


flight_elt_pipeline()
