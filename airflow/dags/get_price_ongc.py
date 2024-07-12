from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import yfinance as yf
import include.sql.yahoo_finance as sql_stmts
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

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
    html_msg = """
    <html>
      <body>
        <h2>Task Execution Notification</h2>
        <p>
          <strong>Task State:</strong> {state}<br>
          <strong>DAG:</strong> {dag}<br>
          <strong>Task:</strong> {task}<br>
          <strong>Execution Time:</strong> {exec_date}<br>
          <strong>Log URL:</strong> <a href="{log_url}">{log_url}</a><br>
        </p>
      </body>
    </html>
    """.format(
        state=task_state,
        dag=task_id,
        task=dag_id,
        exec_date=task_exec_date,
        log_url=task_log_url
    )
    slack_task = SlackWebhookOperator(
        task_id='slack_notification',
        slack_webhook_conn_id='slack',
        message=slack_msg,
        channel="#snowtrend-notify")
    slack_task.execute(context=context)

def pull_xcom(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids = 'Check-Table-Exists-Or-Not', key = 'return_value')
    return result[0].get('EXIST_FLAG')

def decide_branch(**kwargs) :
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids = 'Pull-Pervious-XCOM', key = 'return_value')
    if result :
        return ['Check-Max-Date']
    else :
        return ['Create-Table','Check-Max-Date']

def extract_data(**kwargs) :
    get_stock_history = yf.Ticker(kwargs['ticker'])
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids = 'Check-Max-Date', key = 'return_value')
    result = result[0].get('DATE')
    if result != None :
        start_date = result + timedelta(days=1)
    else :
        start_date = '1900-01-01'
    end_date = datetime.now()
    print('Start Date ---------->',start_date)
    print('End Date ---------->',end_date)
    stock_df = get_stock_history.history(start=start_date, 
                                    end=end_date)
    if stock_df.size != 0 :
        stock_df.reset_index(inplace = True)
        stock_df.rename(
            columns={
                'Low' : 'LOW',
                'High' : 'HIGH',
                'Close' : 'CLOSE',
                'Open' : 'OPEN',
                'Date' : 'DATE',
                'Volume' : 'VOLUME',
                'Dividends' : 'DIVIDENDS',
                'Stock Splits' : 'SPLITS'
            },inplace = True
        )
        stock_df['LOAD_DATE'] = datetime.now()
        conn_obj = SnowflakeHook(
            snowflake_conn_id = "snowflake_db",
            database  = "RAW",
            schema = "STOCK"
        ).get_sqlalchemy_engine()
        stock_df.to_sql('ONGC', conn_obj, if_exists='append', index=False)

default_args = {
    "owner": "Yash",
    "retries": 0,
    # "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notification,
    "on_success_callback": notification
}

with DAG(
    "Load-Data-ONGC",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args = default_args
) as dag :

    check_table_exists = SnowflakeOperator(
            task_id = 'Check-Table-Exists-Or-Not',
            sql=sql_stmts.check_if_exists,
            params = {
                "schema" : 'STOCK',
                "catalog" : 'RAW',
                "table" : 'ONGC',
                "database" : 'RAW'
                        },
            snowflake_conn_id = "snowflake_db"
    )

    pull_xcoms = PythonOperator(
        task_id = 'Pull-Pervious-XCOM',
        python_callable = pull_xcom
    )

    branch_check = BranchPythonOperator(
        task_id = 'Branch-Check',
        python_callable = decide_branch
    )

    create_table = SnowflakeOperator(
            task_id = 'Create-Table',
            sql=sql_stmts.create_stock_table,
            params = {
                "table" : 'ONGC',
                "database" : 'RAW',
                "schema" : 'STOCK'
                        },
            snowflake_conn_id = "snowflake_db"
    )

    load_data = PythonOperator(
        task_id = 'Extract-And-Load-Data',
        python_callable = extract_data,
        op_kwargs = {
                "ticker" : 'ONGC.NS'
                        },
        trigger_rule="none_failed"
    )

    check_max_date = SnowflakeOperator(
        task_id = 'Check-Max-Date',
        sql=sql_stmts.check_max_date,
        params = {
                "table" : 'ONGC',
                "database" : 'RAW',
                "schema" : 'STOCK'
                        },
        snowflake_conn_id = "snowflake_db"
    )

check_table_exists >> pull_xcoms >> branch_check
branch_check >> [create_table,check_max_date] >> load_data