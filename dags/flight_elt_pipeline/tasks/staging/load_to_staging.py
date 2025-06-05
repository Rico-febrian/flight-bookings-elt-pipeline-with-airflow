from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flight_elt_pipeline.tasks.staging.components.load import Load

def load(incremental):
    @task_group
    def load_to_staging():
        """
        Task group to load data from MinIO into the PostgreSQL staging database.

        For each table listed in the Airflow Variable 'tables_to_extract', this task group creates a 
        PythonOperator that loads corresponding CSV data from MinIO into the 'stg' schema in the PostgreSQL warehouse.

        The tasks are chained sequentially (using `>>`) to enforce an explicit load order.

        Args:
            incremental (bool or str): 
                If True or a non-empty string, load only the CSV file with date suffix (e.g., `table-2024-01-01.csv`).
                If False or empty, load the full table version (e.g., `bookings_data.csv`).

        Returns:
            tuple: A tuple containing the first and last tasks in the chain.
        
        """

        # Get list of table names to load from Airflow Variable
        # Example expected value: ['bookings', 'airport]
        tables_to_load = eval(Variable.get('tables_to_extract'))

        # Get dictionary mapping table names to primary keys
        # Example expected value: {"bookings": "booking_id", "airports": ["code", "airport_name"]}
        tables_pkey = eval(Variable.get('tables_to_load'))

        # Define previous and first task value to set up sequential running
        previous_task = None
        first_task = None

        for table_name in tables_to_load:

            # Create PythonOperator for load task
            current_task = PythonOperator(
                task_id = f'load_{table_name}',
                python_callable = Load.load_to_staging,
                trigger_rule = 'none_failed', # Task will run if no previous tasks failed
                op_kwargs = {
                    'table_name' : table_name,
                    'table_pkey' : tables_pkey,
                    'incremental' : incremental
                }
            )

            # Chain task sequentially
            if previous_task:
                previous_task >> current_task
            else:
                first_task = current_task
            previous_task = current_task

        # Return both ends of the chain so the DAG can hook other task groups properly
        return first_task, previous_task

    return load_to_staging()


