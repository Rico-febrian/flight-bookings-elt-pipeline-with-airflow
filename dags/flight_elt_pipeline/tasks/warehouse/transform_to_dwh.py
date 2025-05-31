from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flight_elt_pipeline.tasks.warehouse.components.transform import Transform


def transform():
    @task_group
    def transform_to_dwh():
        tables_to_transform = eval(Variable.get('tables_to_transform'))
        previous_task = None
        first_task = None

        for table_name in tables_to_transform:
            current_task = PythonOperator(
                task_id = f'transform_{table_name}',
                python_callable = Transform.transform_to_dwh,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name' : table_name,
                    'query_path' : f"flight_elt_pipeline/transformation_models/{table_name}.sql"
                }
            )

            if previous_task:
                previous_task >> current_task
            else:
                first_task = current_task
            previous_task = current_task

        return first_task, previous_task

    return transform_to_dwh()


