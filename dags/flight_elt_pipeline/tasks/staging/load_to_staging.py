from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flight_elt_pipeline.tasks.staging.components.load import Load

def load(incremental):
    @task_group
    def load_to_staging():
        tables_to_load = eval(Variable.get('tables_to_extract'))
        tables_pkey = eval(Variable.get('tables_to_load'))
        previous_task = None

        for table_name in tables_to_load:
            current_task = PythonOperator(
                task_id = f'load_{table_name}',
                python_callable = Load.load_to_staging,
                trigger_rule = 'none_failed',
                op_kwargs = {
                    'table_name' : table_name,
                    'table_pkey' : tables_pkey,
                    'incremental' : incremental
                }
            )

            if previous_task:
                previous_task >> current_task

            previous_task = current_task

        return previous_task

    return load_to_staging()


