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

stock_PORTFOLIO_mov_query = """
        SELECT 
        CONCAT(WEEk_START_DATE,' To ',WEEk_END_DATE) DATE_RANGE,
        ACCOUNT_NUMBER,PAN_CARD,
        SUM(ACTUAL_LOT_PRICE) ACTUAL_PORTFOLIO, 
        SUM(LOT_START_PRICE) WEEK_START_PORTFOLIO,
        SUM(LOT_END_PRICE) WEEK_END_PORTFOLIO,
        ROUND(((SUM(LOT_END_PRICE) - SUM(LOT_START_PRICE))/SUM(LOT_START_PRICE)) * 100,2) PER_CHANGE 
        FROM (
        SELECT
        ACCOUNT_NUMBER,PAN_CARD,ISIN,COMPANY,`Security Id` as TICKER,
        AVG_PRICE * TOTAL_QUANTITY as ACTUAL_LOT_PRICE,
        WEEK_START_PRICE * TOTAL_QUANTITY as LOT_START_PRICE,
        WEEK_END_PRICE * TOTAL_QUANTITY as LOT_END_PRICE,
        TOTAL_QUANTITY,WEEk_START_DATE,WEEk_END_DATE
        FROM 
        (WITH ALL_BUY as
        (SELECT ACCOUNT_NUMBER,PAN_CARD,MAX(COMPANY) COMPANY,
        SUM(QUANTITY) TOTAL_QUANTITY,
        ROUND(AVG(PRICE),2) AVG_PRICE,
        ISIN  FROM GROWW.CONTRACT_NOTES
        WHERE `BUY_SELL` = 'Buy'
        GROUP BY  ACCOUNT_NUMBER,PAN_CARD,ISIN)
        ,
        ALL_SELL as
        (SELECT ACCOUNT_NUMBER,PAN_CARD,MAX(COMPANY) COMPANY,
        SUM(QUANTITY) TOTAL_QUANTITY,
        ROUND(AVG(PRICE),2) AVG_PRICE,
        ISIN  FROM GROWW.CONTRACT_NOTES
        WHERE `BUY_SELL` = 'Sell'
        GROUP BY  ACCOUNT_NUMBER,PAN_CARD,ISIN)
        SELECT ab.ACCOUNT_NUMBER,ab.PAN_CARD,ab.COMPANY,IF(`as`.TOTAL_QUANTITY is not null,ab.TOTAL_QUANTITY-`as`.TOTAL_QUANTITY,ab.TOTAL_QUANTITY) TOTAL_QUANTITY,ab.AVG_PRICE,ab.ISIN FROM ALL_BUY ab
        LEFT JOIN
        ALL_SELL `as`
        ON
        ab.ISIN = `as`.ISIN) as gcn
        LEFT JOIN
        STOCK.STOCK_INFO as si
        ON
        gcn.ISIN = si.ISIN_No 
        LEFT JOIN
        (
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
        )  as b where WEEK_END_DATE is not null
        )
        SELECT TICKER,DATE,WEEK_START_PRICE,WEEK_END_PRICE,(SELECT WEEK_START_DATE FROM MARKET_OPEN_DATE) WEEK_START_DATE ,(SELECT WEEK_END_DATE FROM MARKET_OPEN_DATE) WEEK_END_DATE FROM (
        SELECT TICKER,DATE,LEAD(CLOSE, 1) OVER(ORDER BY TICKER,DATE DESC) as WEEK_START_PRICE,CLOSE AS WEEK_END_PRICE,
        IF(LEAD(TICKER, 1) OVER(ORDER BY TICKER,DATE DESC)=TICKER,TRUE,FALSE) as TICKER_FLAG
        FROM (
        SELECT * FROM STOCK_GOLD.STOCK WHERE DATE = (SELECT WEEK_START_DATE FROM MARKET_OPEN_DATE)
        UNION
        SELECT * FROM STOCK_GOLD.STOCK WHERE DATE = (SELECT WEEK_END_DATE FROM MARKET_OPEN_DATE)
        ) as a 
        ) as b WHERE TICKER_FLAG = 1
        ) as s
        on
        si.`Security Id` = SUBSTRING_INDEX(s.TICKER,'.',1)
        ) as f group by ACCOUNT_NUMBER,PAN_CARD;
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
        <tr>
        <th>DATE_RANGE</th>
        <th>ACCOUNT_NUMBER</th>
        <th>PAN_CARD</th>
        <th>ACTUAL_PORTFOLIO</th>
        <th>WEEK_START_PORTFOLIO</th>
        <th>WEEK_END_PORTFOLIO</th>
        <th>PER_CHANGE</th>
        </tr>  
    """
    ti = kwargs['ti']
    task_id  = kwargs['step']
    stock_mov_data = ti.xcom_pull(task_ids = task_id, key = 'return_value')
    for data in stock_mov_data :
        mail_body += """
                    <tr>
                    <td>{date}</td>
                    <td>{accoutno}</td>
                    <td>{pancard}</td>
                    <td>{actual_PORTFOLIO}</td>
                    <td>{weeK_start_PORTFOLIO}</td>
                    <td>{weeK_end_PORTFOLIO}</td>
                    <td>{perchange}</td>
                    </tr> 
                    """.format(date=data[0],
                               accoutno = data[1],
                               pancard =data[2],
                               actual_PORTFOLIO =data[3],
                               weeK_start_PORTFOLIO =data[4],
                               weeK_end_PORTFOLIO =data[5],
                               perchange =data[6]
                               )
    mail_body +=  "</table></body</html>"
    send_email = EmailOperator(
    task_id='send_email',
    to=['yashupadhyaya01@gmail.com','upadhyaydaksh96@gmail.com'],
    subject=kwargs['subject'],
    html_content=mail_body
    )
    send_email.execute(context='')

with DAG(
    "Percent-Change-In-Stock-PORTFOLIO",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * 6",
    catchup=False,
    default_args = default_args
) as dag :
    
    stock_PORTFOLIO_mov = MySqlOperator(
            task_id = 'Calculate-PORTFOLIO-This-Week-Movement',
            sql=stock_PORTFOLIO_mov_query,
            mysql_conn_id = "snow_trend_db"
    )


    stock_PORTFOLIO_slack_message = SqlToSlackWebhookOperator(
        task_id="PORTFOLIO-Channel-Notify",
        sql_conn_id="snow_trend_db",
        sql=stock_PORTFOLIO_mov_query,
        slack_channel="#stock-movement",
        slack_webhook_conn_id="slack_sm",
        slack_message="""PORTFOLIO MOVEMENT :
        {{ results_df }}
        """
    )


    PORTFOLIO_mov_send_email = PythonOperator(
            task_id = 'PORTFOLIO-Send-Email',
            python_callable = create_message,
            op_kwargs = {
            "subject" : 'PORTFOLIO MOVEMENT ALERT',
            "step" : 'Calculate-PORTFOLIO-This-Week-Movement'
                    }
    )



stock_PORTFOLIO_slack_message
stock_PORTFOLIO_mov >> PORTFOLIO_mov_send_email