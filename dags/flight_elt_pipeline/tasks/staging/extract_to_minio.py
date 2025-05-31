from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from helper.logger import logger
from flight_elt_pipeline.tasks.staging.components.extract import Extract
from airflow.models import Variable


# Initialize the logger instance for this DAG
# logger = logger(logger_name="main-dag-log")

def extract(incremental):    
    @task_group
    def extract_to_minio():
        """
        Task group to extract data from source database.

        For each table in source_tables, runs a PythonOperator to
        execute extraction logic, saving results to the bucket.

        Returns a list of extraction tasks.
        """
        
        # Get list tables to extract from Airflow variables
        tables_to_extract = eval(Variable.get('tables_to_extract'))
        tasks = []

        for table_name in tables_to_extract:
            current_task = PythonOperator(
                task_id=f"extract_{table_name}",
                python_callable=Extract.source_db,
                trigger_rule = 'none_failed',
                op_kwargs={
                    'table_name' : table_name,
                    'incremental' : incremental
                }
            )
            
            tasks.append(current_task)

        return tasks

    return extract_to_minio()

