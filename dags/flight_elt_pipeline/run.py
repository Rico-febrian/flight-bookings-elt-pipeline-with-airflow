from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helper.logger import logger
from flight_elt_pipeline.tasks.staging.extract_to_minio import extract
from flight_elt_pipeline.tasks.staging.load_to_staging import load
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
    start_date=datetime(2025, 5, 18),
    schedule="@once",
    catchup=False,
    default_args=default_args
)

def flight_elt_pipeline():
    incremental_mode = eval(Variable.get('incremental'))

    extract_task_group = extract(incremental=incremental_mode)
    load_task_group = load(incremental=incremental_mode)

    extract_task_group >> load_task_group

flight_elt_pipeline()

# def flight_elt_pipeline():
#     """
#     Main DAG function for the travel ELT pipeline.

#     This DAG orchestrates the ETL pipeline composed of three phases:
#     1. Extract: Extract data from source DB and save to a storage bucket.
#     2. Load: Load extracted data into staging schema in the warehouse.
#     3. Transform: Run transformations and load data into final DWH schema.

#     Uses Airflow task groups for logical grouping of each phase's tasks,
#     and chains them sequentially with clear start and end markers.
#     """
    
#     # Connection IDs and configuration
#     connection_id_extract = "source_db"
#     connection_id_load = "warehouse_db"
#     connection_id_dwh = "warehouse_db"
#     stg_schema = "stg"
#     dwh_schema = "final"

#     @task_group
#     def extract(incremental):
#         """
#         Task group to extract data from source database.

#         For each table in source_tables, runs a PythonOperator to
#         execute extraction logic, saving results to the bucket.

#         Returns a list of extraction tasks.
#         """

#         # Define MinIO bucket name
#         bucket_name = "extracted-data"
        
#         # Get list tables to extract from Airflow variables
#         tables_to_extract = eval(Variable.get('tables_to_extract'))

#         for table_name in tables_to_extract:
#             current_task = PythonOperator(
#                 task_id=f"extract_{table_name}",
#                 python_callable=Extract.source_db,
#                 op_kwargs={
#                     'table_name' : f'{table_name}',
#                     'bucket_name' : f'{bucket_name}'
#                     'incremental' : incremental
#                 },
#             )
            
#             current_task

#     # @task_group
#     # def load():
#     #     """
#     #     Task group to load extracted data into the staging schema.

#     #     Creates a sequence of PythonOperators that load each extracted table
#     #     into the staging schema in the warehouse DB.

#     #     Returns a tuple of (first_task, last_task) to enable chaining.
#     #     """
#     #     previous_task = None
#     #     first_task = None
#     #     for table_name in source_tables:
#     #         try:
#     #             logger.info(f"Scheduling load task for table: {table_name}")

#     #             task = PythonOperator(
#     #                 task_id=f"load_{table_name}",
#     #                 python_callable=Load.load_to_staging,
#     #                 op_kwargs={
#     #                     "connection_id": connection_id_load,
#     #                     "bucket_name": bucket_name,
#     #                     "schema": stg_schema,
#     #                     "table_name": table_name,
#     #                 },
#     #             )
#     #             if previous_task:
#     #                 previous_task >> task
#     #             else:
#     #                 first_task = task
#     #             previous_task = task
#     #         except Exception as e:
#     #             logger.error(f"Failed to schedule load for {table_name}: {e}", exc_info=True)
#     #             raise
#     #     return first_task, previous_task

#     # @task_group
#     # def transform():
#     #     """
#     #     Task group to transform and load data from staging to final DWH schema.

#     #     Runs transformations sequentially for each table in dwh_tables,
#     #     calling the Transform.transform_to_dwh method.

#     #     Returns the first transformation task for chaining.
#     #     """
#     #     previous_task = None
#     #     first_task = None
#     #     for table_name in dwh_tables:
#     #         try:
#     #             logger.info(f"Scheduling transform task for table: {table_name}")

#     #             task = PythonOperator(
#     #                 task_id=f"transform_{table_name}",
#     #                 python_callable=Transform.transform_to_dwh,
#     #                 op_kwargs={
#     #                     "connection_id": connection_id_dwh,
#     #                     "query_path": f"travel_elt_pipeline/transformation_models/{table_name}.sql",
#     #                     "schema": dwh_schema,
#     #                     "table_name": table_name,
#     #                 },
#     #             )
#     #             if previous_task:
#     #                 previous_task >> task
#     #             else:
#     #                 first_task = task
#     #             previous_task = task
#     #         except Exception as e:
#     #             logger.error(f"Failed to schedule transform for {table_name}: {e}", exc_info=True)
#     #             raise
#     #     return first_task

#     # Start and end control tasks for clear DAG structure
#     start = EmptyOperator(task_id="start")
#     extract_done = EmptyOperator(task_id="extract_done")
#     # load_done = EmptyOperator(task_id="load_done")
#     # transform_done = EmptyOperator(task_id="transform_done")
#     end = EmptyOperator(task_id="end")

#     # Execute task groups
#     extract_tasks = extract()
#     # load_first_task, load_last_task = load()
#     # transform_first_task = transform()

#     # Chain tasks for correct flow: start -> extract -> extract_done -> load -> load_done -> transform -> transform_done -> end
#     start >> extract_tasks
#     for task in extract_tasks:
#         task >> extract_done
#     # extract_done >> load_first_task
#     # load_last_task >> load_done
#     # load_done >> transform_first_task
#     # transform_first_task >> transform_done
#     # transform_done >> end

# flight_elt_pipeline()


