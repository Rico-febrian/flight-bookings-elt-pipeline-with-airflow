from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flight_elt_pipeline.tasks.warehouse.components.transform import Transform


def transform():
    @task_group
    def transform_to_dwh():
        """
        Task group to run transformation SQL queries that load data from the staging schema into the data warehouse.

        For each table listed in the Airflow Variable 'tables_to_transform', this task group creates a 
        PythonOperator that runs a transformation SQL file using the Transform.transform_to_dwh function.

        The tasks are chained in sequence (using `>>`) to preserve the dependency order between tables.

        Returns:
            tuple: A tuple containing the first and last tasks in the chain. 
        """

        # Get list of tables to transform from Airflow Variable
        # Example expected value: ['dim_airport', 'fct_bookings']
        tables_to_transform = eval(Variable.get('tables_to_transform'))
        
        # Define previous and first task value to set up sequential running
        previous_task = None
        first_task = None

        for table_name in tables_to_transform:
            # Define path to the SQL transformation file for each table
            query_path = f"flight_elt_pipeline/transformation_models/{table_name}.sql"

            # Create PythonOperator for transformation task
            current_task = PythonOperator(
                task_id = f'transform_{table_name}',
                python_callable = Transform.transform_to_dwh,
                trigger_rule = 'none_failed', # Run only if previous task succeeded
                op_kwargs = {
                    'table_name' : table_name,
                    'query_path' : query_path
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

    return transform_to_dwh()


