from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pdfminer.layout import LAParams, LTTextBox
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
import pandas as pd
from PyPDF2 import PdfFileReader, PdfFileWriter
import numpy as np
from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime
import os
import base64
import re
from airflow.providers.mysql.hooks.mysql import MySqlHook
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import yfinance as yf

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

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


def make_contract_note_dic(**kwargs) :
    contract_conf = {}
    ti = kwargs['ti']
    contract_conf_data = ti.xcom_pull(task_ids = 'Fetch-Contract-Config' ,key ='return_value')
    for data in contract_conf_data :
       contract_conf[data[0]] = data[1]
    ti.xcom_push(value = contract_conf ,key ='contract_conf')

def extract_contract_note_from_mail(**kwargs):
  step = kwargs['step']
  email = kwargs['email']

  creds = None
  if os.path.exists("/root/airflow/token.json"):
    creds = Credentials.from_authorized_user_file("/root/airflow/token.json", SCOPES)
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "/root/airflow/credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    with open("/root/airflow/token.json", "w") as token:
      token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)
    results = service.users().messages().list(userId='me').execute() 
    messages = results.get('messages') 
    for msg in messages: 
        message = service.users().messages().get(userId='me', id=msg['id']).execute()  
        payload = message['payload'] 
        headers = payload['headers'] 
        for d in headers: 
            if d['name'] == 'Subject': 
                subject = d['value'] 
            if d['name'] == 'From': 
                sender = d['value'] 

        if sender in [email] or 'contract note for' in subject.lower() :
            file_date = re.search(r'\d{2}-\d{2}-\d{4}', subject)
            file_date = file_date.group()
            parts = payload.get('parts')
            for part in parts :
                if part['filename']:
                    attachment = service.users().messages().attachments().get(userId='me', messageId=message['id'],
                                                    id=part['body']['attachmentId']).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    store_dir = '/root/airflow/contract_notes/'+step+'/'
                    path = ''.join([store_dir, part['filename'].rsplit('.',1)[0],'_',file_date,'.pdf'])
                    f = open(path, 'wb')
                    f.write(file_data)
                    f.close()

def extract_contract_notes(**kwargs) :
   ti = kwargs['ti']
   contract_conf = ti.xcom_pull(task_ids = 'Contract-Note-Conf-Dic',key = 'contract_conf')
   step = kwargs['step']
   processed_file = ti.xcom_pull(task_ids = 'Fetch-Processed-Files',key = 'return_value')
   processed_file_list = []
   for file in processed_file :
       processed_file_list.append(file[0])
       
   for files in os.listdir('/root/airflow/contract_notes/'+step) :
        if files not in processed_file_list :
            out = PdfFileWriter()
            reader = PdfFileReader('/root/airflow/contract_notes/'+step+'/'+files)
            if reader.isEncrypted:
                account_number = re.search(r'[0-9]{8,16}', files)
                account_number = account_number.group()
                password = contract_conf.get(account_number)
                reader.decrypt(password)
            for idx in range(reader.numPages): 
                page = reader.getPage(idx) 
                out.addPage(page) 
                with open('/root/airflow/contract_notes/'+step+'/'+files, "wb") as f: 
                    out.write(f) 

            fp = open('/root/airflow/contract_notes/'+step+'/'+files, 'rb')
            rsrcmgr = PDFResourceManager()
            laparams = LAParams(line_overlap=0.3,char_margin=1,word_margin=0.2,line_margin=0.2,detect_vertical=True,all_texts=True)
            device = PDFPageAggregator(rsrcmgr, laparams=laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            pages = PDFPage.get_pages(fp)
            final_df = pd.DataFrame()
            page_no = 1
            for page in pages:
                    X = 0
                    Y = 0
                    page_df = pd.DataFrame()
                    interpreter.process_page(page)
                    layout = device.get_result()
                    for lobj in layout:
                        if isinstance(lobj, LTTextBox):
                            text = lobj.get_text()    
                            df = pd.DataFrame(columns=['Y0','Y1','X0','X1','DETAIL'])     
                            coordinate = str(lobj.bbox)[1:-1].split(",")
                            df['Y0'] = [coordinate[0]]
                            df['Y1'] = [coordinate[1]]
                            df['X0'] = [coordinate[2]]
                            df['X1'] = [coordinate[3]]
                            df['DETAIL'] = text.replace('\n',' ')
                            page_df = pd.concat([page_df,df],axis=0,ignore_index=True)
                            page_df = page_df.sort_values(by=['X1','X0'],ignore_index=False)
                    co_ordinated_dic = {}
                    page_df['KEY'] = page_df['Y1'] + '_' + page_df['X1']
                    isin_df = pd.DataFrame()
                    for data in page_df.itertuples() :
                        if 'INE' in data[5] :
                            temp_isin_df = pd.DataFrame()
                            temp_isin_df['X1'] = [data[4]]
                            temp_isin_df['ISIN'] = [data[5]]
                            isin_df = pd.concat([isin_df,temp_isin_df],axis=0,ignore_index=True)
                            isin_df['LAGGED'] = isin_df['X1'].shift(-1)
                    key_df = page_df['KEY'].unique()
                    for key in key_df:
                        if page_df['KEY'].value_counts()[key] > 10 :
                            co_ordinated_dic[key] = page_df['KEY'].value_counts()[key]
                    if co_ordinated_dic != {} :
                        description_df = pd.DataFrame()   
                        for filter_key in co_ordinated_dic.keys():
                            temp_df = pd.DataFrame()
                            temp_df = page_df.loc[(page_df.KEY == filter_key)]
                            description_df = pd.concat([description_df,temp_df],axis=0,ignore_index=True)
                        description_isin_df = pd.DataFrame()
                        for data in isin_df.itertuples() :
                            df = pd.DataFrame()
                            df = description_df[(description_df['X1'] > str(data[1])) & (description_df['X1'] < str(data[3]))] 
                            df.reset_index(drop=True,inplace=True)
                            indexes_to_insert = df.index[df.index % 11 == 0]
                            sorted_indices = sorted(indexes_to_insert, reverse=True)
                            for i, index in enumerate(sorted_indices):
                                # Insert the new data
                                df = pd.concat([df.iloc[:index],pd.Series(data[2]),df.iloc[index :]], ignore_index=True)
                            df['DETAIL'] = df[0].fillna(df['DETAIL'])
                            no_of_table_rows = int(len(df)//12)
                            if page_df.size > 0 :
                                # df = df.sort_values(by=['X1','Y0'],ignore_index=False)
                                final_df_details = df['DETAIL']             
                                file_df = pd.DataFrame()
                                # print(final_df_details)
                                for item in np.array_split(final_df_details, (no_of_table_rows)):
                                    temp_df = pd.DataFrame(item)
                                    temp_df.reset_index(drop=True,inplace=True)
                                    temp_df = temp_df.transpose()
                                    file_df = pd.concat([file_df,temp_df],axis=0)
                                    # print(temp_df)
                                page_no += 1
                                final_df = pd.concat([final_df,file_df],axis=0,ignore_index=True)
            # print(final_df.to_string())
            final_df.rename(columns={0:'ISIN',
                                    1:'Price',
                                    4:'OrderNo',
                                    5:'TradeTime',
                                    6:'TradeNo',
                                    8:'Company',
                                    9:'Exchange',
                                    10:'Buy/Sell',
                                    11:'Quantity',
                                    },inplace=True)
            final_df['CONTRACT_NOTE_DATE'] = datetime.strptime(files.rsplit('_',1)[1].rsplit('.pdf')[0],'%d-%m-%Y')
            final_df['ACCOUNT_NUMBER'] = account_number
            final_df['PAN_CARD'] = password
            final_df = final_df[['CONTRACT_NOTE_DATE','ACCOUNT_NUMBER','PAN_CARD','Company','TradeTime','OrderNo','TradeNo','Buy/Sell','Quantity','Price','Exchange','ISIN']]
            final_df['FILE_NAME'] = files
            final_df['LOAD_DATE'] = str(datetime.now())
            final_df['Buy/Sell'] = final_df['Buy/Sell'].apply(lambda x: 'Buy' if x.strip() == 'B' else 'Sell')
            final_df = final_df.replace(r'\n','', regex=True) 
            final_df['ACCOUNT_NUMBER'] = final_df['ACCOUNT_NUMBER'].str.strip()
            final_df['PAN_CARD'] = final_df['PAN_CARD'].str.strip()
            final_df['Company'] = final_df['Company'].str.strip()
            final_df['TradeTime'] = final_df['TradeTime'].str.strip()
            final_df['OrderNo'] = final_df['OrderNo'].str.strip()
            final_df['TradeNo'] = final_df['TradeNo'].str.strip()
            final_df['Buy/Sell'] = final_df['Buy/Sell'].str.strip()
            final_df['Quantity'] = final_df['Quantity'].str.strip()
            final_df['Price'] = final_df['Price'].str.strip()
            final_df['Exchange'] = final_df['Exchange'].str.strip()
            final_df['ISIN'] = final_df['ISIN'].str.strip()
            final_df['FILE_NAME'] = final_df['FILE_NAME'].str.strip()

            conn_obj = MySqlHook(
                mysql_conn_id  = "snow_trend_db"
            ).get_conn()
            cursor_obj = conn_obj.cursor()
            insert_query = 'INSERT INTO GROWW.CONTRACT_NOTES VALUE {record}'
            for data in final_df.itertuples() :
                # print(insert_query.format(record=data[1:]))
                cursor_obj.execute(insert_query.format(record=data[1:]))
            conn_obj.commit()
            conn_obj.close()
        
        else :
            print('Already Processed File ----------->',files)

with DAG(
    "Contract-Note-Extract",
    start_date=datetime(2024, 1, 1),
    schedule="0 20 * * *",
    catchup=False,
    default_args = default_args
) as dag :
  
    extract_grow_config = MySqlOperator(
        task_id = 'Fetch-Contract-Config',
        sql="SELECT * FROM GROWW.CONTRACT_NOTE_CONF",
        mysql_conn_id = "snow_trend_db"
        )

    contract_note_dic = PythonOperator(
            task_id = 'Contract-Note-Conf-Dic',
            python_callable = make_contract_note_dic,
        )
   
    groww_extact_contract = PythonOperator(
            task_id = 'Extract-Contract-Notes-From-Gmail',
            python_callable = extract_contract_note_from_mail,
            op_kwargs = {
            "step" : 'groww',
            "email" : 'noreply@groww.in'
                    }
        )

    extact_contract_notes = PythonOperator(
            task_id = 'Extract-Contract-Notes-Data',
            python_callable = extract_contract_notes,
            op_kwargs = {
            "step" : 'groww',
            "email" : 'noreply@groww.in'
                    }
        )
    
    fetch_processed_files = MySqlOperator(
        task_id = 'Fetch-Processed-Files',
        sql = 'SELECT DISTINCT(FILE_NAME) FROM GROWW.CONTRACT_NOTES',
        mysql_conn_id = "snow_trend_db"
    )


   
extract_grow_config >> contract_note_dic  >> groww_extact_contract >> fetch_processed_files >> extact_contract_notes