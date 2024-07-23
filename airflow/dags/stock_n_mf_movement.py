from airflow import DAG
from datetime import datetime, timedelta
# from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.transfers.sql_to_slack_webhook import SqlToSlackWebhookOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'Yash',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

stock_mov_query = """
            WITH MARKET_OPEN_DATE AS
            (
            SELECT * FROM (
            SELECT WEEK_START_DATE,LEAD(WEEK_START_DATE, 1) OVER(ORDER BY WEEK_START_DATE ASC ) WEEK_END_DATE
            FROM
            (
            SELECT MAX(DATE) WEEK_START_DATE FROM STOCK_GOLD.STOCK WHERE
            WEEKOFYEAR(DATE) = (SELECT WEEKOFYEAR(current_date)-1) and YEAR(DATE) = YEAR(CURRENT_DATE)
            UNION 
            SELECT MAX(DATE) WEEK_START_DATE FROM STOCK_GOLD.STOCK WHERE
            WEEKOFYEAR(DATE) = (SELECT WEEKOFYEAR(current_date)) and YEAR(DATE) = YEAR(CURRENT_DATE)
            ) as a
            ) as b where WEEK_END_DATE is not null)
            SELECT (SELECT concat(DATE(WEEK_START_DATE),' TO ',DATE(WEEK_END_DATE)) FROM MARKET_OPEN_DATE) DATE_RANGE,
            TICKER,
            ROUND((WEEK_END_PRICE - WEEK_START_PRICE)/WEEK_START_PRICE * 100,2) PER_CHANGE
            FROM (
            SELECT TICKER,DATE,CLOSE as WEEK_END_PRICE,LEAD(CLOSE, 1) OVER(ORDER BY TICKER,DATE DESC) WEEK_START_PRICE FROM STOCK_GOLD.STOCK where
            DATE = (SELECT WEEK_START_DATE FROM MARKET_OPEN_DATE) or DATE = (SELECT WEEK_END_DATE FROM MARKET_OPEN_DATE)
            ORDER BY TICKER,DATE DESC
            ) as a WHERE DATE = (SELECT WEEK_END_DATE FROM MARKET_OPEN_DATE);
            """

mf_mov_query = """
            WITH MARKET_OPEN_DATE AS
            (
            SELECT * FROM (
            SELECT WEEK_START_DATE,LEAD(WEEK_START_DATE, 1) OVER(ORDER BY WEEK_START_DATE ASC ) WEEK_END_DATE
            FROM
            (
            SELECT MAX(DATE) WEEK_START_DATE FROM STOCK_GOLD.MF WHERE
            WEEKOFYEAR(DATE) = (SELECT WEEKOFYEAR(current_date)-1) and YEAR(DATE) = YEAR(CURRENT_DATE)
            UNION 
            SELECT MAX(DATE) WEEK_START_DATE FROM STOCK_GOLD.MF WHERE
            WEEKOFYEAR(DATE) = (SELECT WEEKOFYEAR(current_date)) and YEAR(DATE) = YEAR(CURRENT_DATE)
            ) as a
            ) as b where WEEK_END_DATE is not null)
            SELECT (SELECT concat(DATE(WEEK_START_DATE),' TO ',DATE(WEEK_END_DATE)) FROM MARKET_OPEN_DATE) DATE_RANGE,
            TICKER,
            ROUND((WEEK_END_PRICE - WEEK_START_PRICE)/WEEK_START_PRICE * 100,2) PER_CHANGE
            FROM (
            SELECT TICKER,DATE,CLOSE as WEEK_END_PRICE,LEAD(CLOSE, 1) OVER(ORDER BY TICKER,DATE DESC) WEEK_START_PRICE FROM STOCK_GOLD.MF where
            DATE = (SELECT WEEK_START_DATE FROM MARKET_OPEN_DATE) or DATE = (SELECT WEEK_END_DATE FROM MARKET_OPEN_DATE)
            ORDER BY TICKER,DATE DESC
            ) as a WHERE DATE = (SELECT WEEK_END_DATE FROM MARKET_OPEN_DATE);
            """

def create_message(**kwargs) :
    mail_body = """
        <html>  
        <head>
        <style>  
        table, th, td {  
            border: 1px solid black;  
            border-collapse: collapse;  
        }  
        th, td {  
            padding: 10px;  
        }  
        table#alter tr:nth-child(even) {  
            background-color: #eee;  
        }  
        table#alter tr:nth-child(odd) {  
            background-color: #fff;  
        }  
        table#alter th {  
            color: white;  
            background-color: gray;  
        }  
        </style>  
        </head>
        <body>  
        <table id="alter">  
        <tr><th>DATE_RANGE</th><th>TICKER</th><th>PER_CHANGE</th></tr>  
    """
    ti = kwargs['ti']
    task_id  = kwargs['step']
    stock_mov_data = ti.xcom_pull(task_ids = task_id, key = 'return_value')
    for data in stock_mov_data :
        print(data)
        mail_body += """
                    <tr><td>{date}</td><td>{company}</td><td>{change}</td></tr> 
                    """.format(date=data[0],
                               company = data[1],
                               change =data[2]
                               )
    mail_body +=  "</table></body</html>"
    send_email = EmailOperator(
    task_id='send_email',
    to=['yashupadhyaya01@gmail.com','upadhyaydaksh96@gmail.com','kushpatel0372@gmail.com'],
    subject=kwargs['subject'],
    html_content=mail_body
    )
    send_email.execute(context='')

with DAG(
    "Percent-Change-In-Stock-Or-MF",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * 6",
    catchup=False,
    default_args = default_args
) as dag :
    
    stock_mov = MySqlOperator(
            task_id = 'Calculate-Stock-This-Week-Movement',
            sql=stock_mov_query,
            mysql_conn_id = "snow_trend_db"
    )

    mf_mov = MySqlOperator(
            task_id = 'Calculate-MF-This-Week-Movement',
            sql=mf_mov_query,
            mysql_conn_id = "snow_trend_db"
    )


    stock_slack_message = SqlToSlackWebhookOperator(
        task_id="Stock-Channel-Notify",
        sql_conn_id="snow_trend_db",
        sql=stock_mov_query,
        slack_channel="#stock-movement",
        slack_webhook_conn_id="slack_sm",
        slack_message="""STOCK MOVEMENT :
        {{ results_df }}
        """
    )

    mf_slack_message = SqlToSlackWebhookOperator(
        task_id="MF-Channel-Notify",
        sql_conn_id="snow_trend_db",
        sql=mf_mov_query,
        slack_channel="#stock-movement",
        slack_webhook_conn_id="slack_sm",
        slack_message="""MUTUAL FUND MOVEMENT :
        {{ results_df }}
        """
    )

    stock_mov_send_email = PythonOperator(
            task_id = 'Stock-Send-Email',
            python_callable = create_message,
            op_kwargs = {
            "subject" : 'STOCK MOVEMENT ALERT',
            "step" : 'Calculate-Stock-This-Week-Movement'
                    }
    )

    mf_mov_send_email = PythonOperator(
            task_id = 'MF-Send-Email',
            python_callable = create_message,
            op_kwargs = {
            "subject" : 'MUTUAL FUND MOVEMENT ALERT',
            "step" : 'Calculate-MF-This-Week-Movement'
                    }
    )


stock_slack_message
mf_slack_message
stock_mov >> stock_mov_send_email
mf_mov >> mf_mov_send_email