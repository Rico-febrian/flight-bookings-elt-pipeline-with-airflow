from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from flight_elt_pipeline.tasks.staging.components.extract import Extract
from airflow.models import Variable

def extract(incremental):    
    @task_group
    def extract_to_minio():
        """
        Task group to extract data from source PostgreSQL database and upload it to MinIO.

        This task group creates a separate task for each table listed in the 'tables_to_extract' Airflow variable.
        Each task calls `Extract.source_db` to perform data extraction and uploads the data as a CSV file to a MinIO bucket.

        The `incremental` flag determines whether the data extraction should be full (entire table)
        or incremental (only new/updated rows from the previous day based on `created_at`/`updated_at` columns).

        Returns:
            list: A list of PythonOperator tasks, one for each table.
        """
        
        # Retrieve list of table names to extract from Airflow Variables
        # The variable is expected to be a stringified list, e.g., "['bookings', 'airports']"
        tables_to_extract = eval(Variable.get('tables_to_extract'))

        tasks = []

        for table_name in tables_to_extract:
            # Define a PythonOperator for each table to extract its data
            current_task = PythonOperator(
                task_id=f"extract_{table_name}",    # Unique task ID per table
                python_callable=Extract.source_db,  # Target extraction function
                trigger_rule='none_failed',         # Allow all parallel tasks; skip downstream only if one fails
                op_kwargs={
                    'table_name': table_name,
                    'incremental': incremental      # Pass incremental flag to Extract.source_db
                }
            )
            
            tasks.append(current_task)

        return tasks

    return extract_to_minio()

