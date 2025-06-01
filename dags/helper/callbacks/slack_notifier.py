from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def slack_notifier(context):
    slack_icon = "red_circle"
    task_state = context.get("task_instance").state
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    task_exec_date = context.get("execution_date")
    task_log_url = context.get("task_instance").log_url
    exception = context.get("exception") if context.get("exception") else "No exception"

    slack_msg = f"""
            :{slack_icon}: *Task {task_state}*
            *Dag*: '{dag_id}'
            *Task*: '{task_id}'
            *Execution Time*: '{task_exec_date}'
            *Log URL*: '{task_log_url}'
            *Exceptions*: '''{exception}'''
            """
    
    slack_webhook_task = SlackWebhookOperator(
        slack_webhook_conn_id = "slack_notifier",
        task_id = "slack_notification",
        message = slack_msg,
        channel = "elt_pipeline"
    )

    slack_webhook_task.execute(context=context)