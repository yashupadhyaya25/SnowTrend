from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
	
class dBtLayerBuildBashOperator(BashOperator):
        ui_color = "#00FFFF"
        ui_fgcolor = "black"

class dBtLayerRunBashOperator(BashOperator):
        ui_color = "#00FF7F"
        ui_fgcolor = "black"

class dBtLayerTestBashOperator(BashOperator):
        ui_color = "#FFA07A"
        ui_fgcolor = "black"

class StockTaskMySqlOperator(MySqlOperator):
        ui_color = "#D8BFD8"
        ui_fgcolor = "black"

class StockTaskPythonOperator(PythonOperator):
        ui_color = "#D8BFD8"
        ui_fgcolor = "black"

class MfTaskMySqlOperator(MySqlOperator):
        ui_color = "#FFDAB9"
        ui_fgcolor = "black"

class MfTaskPythonOperator(PythonOperator):
        ui_color = "#FFDAB9"
        ui_fgcolor = "black"


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

def create_sql_file(**kwargs):
    ti = kwargs['ti']
    step = kwargs['step']
    sql_file_name = kwargs['file_name']
    result = ti.xcom_pull(task_ids = step, key = 'return_value')
    sql_query = """
    {{config(schema = 'RAW')}}
"""
    for ticker in result :
        sql_query += "SELECT * ,'"+ticker[0]+"' TICKER FROM STOCK."+ticker[0]
        if result[-1][0] != ticker[0] :
            sql_query +=  " UNION ALL "

    if os.path.exists('/root/snow_trend/models/raw/'+sql_file_name) :
        os.remove('/root/snow_trend/models/raw/'+sql_file_name)
    with open('/root/snow_trend/models/raw/'+sql_file_name, 'w') as file:
        file.write(sql_query)
    return 'Success'

with DAG(
    "DBT-RUN",
    start_date=datetime(2024, 1, 1),
    schedule="0 16 * * *",
    catchup=False,
    default_args = default_args
) as dag :
    

    with TaskGroup(group_id="dBt-Pre-Build") as dbt_pre_build:
        fetch_stock_table_for_dbt_merge = StockTaskMySqlOperator(
                task_id = 'Fetch-Stock-Table-For-Dbt-Merge',
                sql="""
                    SELECT a.TABLE_NAME FROM (
                    SELECT TABLE_NAME FROM information_schema.tables where table_schema ='STOCK' and table_name not in ('YAHOO_CONF','STOCK_INFO')) as a
                    LEFT JOIN
                    (SELECT IF(IS_MF=1,REPLACE(UPPER(COMPANY_NAME),' ','_'),SUBSTRING_INDEX(TICKER,'.',1)) as TABLE_NAME,IS_MF FROM STOCK.YAHOO_CONF) as b
                    ON a.TABLE_NAME = b.TABLE_NAME WHERE b.IS_MF = 0
                """,
                mysql_conn_id = "snow_trend_db"
        )

        create_all_stock_sql_file_raw = StockTaskPythonOperator(
            task_id = 'Create-ALL-Stock',
            python_callable = create_sql_file,
            op_kwargs = {
            "step" : 'dBt-Pre-Build.Fetch-Stock-Table-For-Dbt-Merge',
            "file_name" : 'ALL_STOCK.sql'
                    }
        )

        fetch_mf_table_for_dbt_merge = MfTaskMySqlOperator(
                task_id = 'Fetch-MF-Table-For-Dbt-Merge',
                sql="""
                    SELECT a.TABLE_NAME FROM (
                    SELECT TABLE_NAME FROM information_schema.tables where table_schema ='STOCK' and table_name not in ('YAHOO_CONF')) as a
                    LEFT JOIN
                    (SELECT IF(IS_MF=1,REPLACE(UPPER(COMPANY_NAME),' ','_'),SUBSTRING_INDEX(TICKER,'.',1)) as TABLE_NAME,IS_MF FROM STOCK.YAHOO_CONF) as b
                    ON a.TABLE_NAME = b.TABLE_NAME WHERE b.IS_MF = 1
                """,
                mysql_conn_id = "snow_trend_db"
        )

        create_all_mf_sql_file_raw = MfTaskPythonOperator(
            task_id = 'Create-ALL-MF',
            python_callable = create_sql_file,
            op_kwargs = {
            "step" : 'dBt-Pre-Build.Fetch-MF-Table-For-Dbt-Merge',
            "file_name" : 'ALL_MF.sql'
                    }
        )

        [fetch_stock_table_for_dbt_merge >> create_all_stock_sql_file_raw,fetch_mf_table_for_dbt_merge >>create_all_mf_sql_file_raw]

    with TaskGroup(group_id="Build-Raw-Layer") as build_raw_layer:

        raw_layer_build = dBtLayerBuildBashOperator(
            task_id='Raw-Layer-Build',
            bash_command='cd /root/snow_trend;dbt build --select=raw',
            dag=dag,
            trigger_rule = 'all_success'
        )

        raw_layer_run = dBtLayerRunBashOperator(
            task_id='Raw-Layer-Run',
            bash_command='cd /root/snow_trend;dbt run --select=raw',
            dag=dag,
            trigger_rule = 'all_success'
        )

        raw_layer_test = dBtLayerTestBashOperator(
            task_id='Raw-Layer-Test',
            bash_command='cd /root/snow_trend;dbt test --select=raw',
            dag=dag,
            trigger_rule = 'all_success'
        )

        raw_layer_build >> raw_layer_run >> raw_layer_test
    
    with TaskGroup(group_id="Build-Bronze-Layer") as build_bronze_layer: 
        bronze_layer_build = dBtLayerBuildBashOperator(
            task_id='Bronze-Layer-Build',
            bash_command='cd /root/snow_trend;dbt build --select=bronze',
            dag=dag,
            trigger_rule = 'all_success'
        )

        bronze_layer_run = dBtLayerRunBashOperator(
            task_id='Bronze-Layer-Run',
            bash_command='cd /root/snow_trend;dbt run --select=bronze',
            dag=dag,
            trigger_rule = 'all_success'
        )

        bronze_layer_test = dBtLayerTestBashOperator(
            task_id='Bronze-Layer-Test',
            bash_command='cd /root/snow_trend;dbt test --select=bronze',
            dag=dag,
            trigger_rule = 'all_success'
        )

        bronze_layer_build >> bronze_layer_run >> bronze_layer_test
    
    with TaskGroup(group_id="Build-Gold-Layer") as build_gold_layer: 
            gold_layer_build = dBtLayerBuildBashOperator(
                task_id='Gold-Layer-Build',
                bash_command='cd /root/snow_trend;dbt build --select=gold',
                dag=dag,
                trigger_rule = 'all_success'
            )

            gold_layer_run = dBtLayerRunBashOperator(
                task_id='Gold-Layer-Run',
                bash_command='cd /root/snow_trend;dbt run --select=gold',
                dag=dag,
                trigger_rule = 'all_success'
            )

            gold_layer_test = dBtLayerTestBashOperator(
                task_id='Gold-Layer-Test',
                bash_command='cd /root/snow_trend;dbt test --select=gold',
                dag=dag,
                trigger_rule = 'all_success'
            )

            gold_layer_build >> gold_layer_run >> gold_layer_test

dbt_pre_build >> build_raw_layer >> build_bronze_layer >> build_gold_layer