from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def notification(context):
    slack_icon = "large_green_circle" if context.get('task_instance').state == "success" else "red_circle"
    task_state = context.get('task_instance').state
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    task_exec_date = context.get('execution_date')
    task_log_url = context.get('task_instance').log_url

    slack_msg = """
            :{icon}: Task {state}.
            *Dag*: {task} 
            *Task*: {dag}  
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        icon=slack_icon,
        state=task_state,
        dag=task_id,
        task=dag_id,
        exec_date=task_exec_date,
        log_url=task_log_url)
    slack_task = SlackWebhookOperator(
        task_id='slack_notification',
        slack_webhook_conn_id='slack',
        message=slack_msg,
        channel="#snowtrend-notify")
    slack_task.execute(context=context)

default_args = {
    "owner": "Yash",
    "retries": 0,
    "on_failure_callback": notification,
    "on_success_callback": notification
}

with DAG(
    "Merge-All-Stock-Data",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args = default_args
) as dag :

    merge_stock_data = BashOperator(
        task_id='Stock-Merge',
        bash_command='cd /root/snow_trend;dbt run --select all_stock',
        dag=dag
    )

    data_test = BashOperator(
        task_id='Data-Test',
        bash_command='cd /root/snow_trend;dbt test --select all_stock;dbt test --select sql_test_all_stock',
        dag=dag
    )

merge_stock_data >> data_test