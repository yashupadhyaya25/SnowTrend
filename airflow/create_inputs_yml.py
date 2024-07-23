import mysql.connector
import yaml
from configparser import ConfigParser

config = ConfigParser()
config.read('airflow_config.ini')
# Create a connection to Snowflake
connection = mysql.connector.connect(
    host=config.get('DEV','host'),
    user=config.get('DEV','user'),
    password=config.get('DEV','password')
)

# Define a cursor
cursor = connection.cursor()

try:
    # select from table where your all yahoo config store
    cursor.execute("SELECT * FROM STOCK.YAHOO_CONF where IS_ACTIVE <> FALSE") 
    result_set = cursor.fetchall()
    for result in result_set :
        company = result[0].replace(' ','_').upper() if result[3] else result[1].split('.')[0].upper()
        config = {'dag_id': company,
              'schedule_interval' : '0 14 * * *',
              'catchup' : False,
              'owner' : 'Yash',
              'database' : 'STOCK',
              'table' : company,
              'ticker' : result[1]
              }
        with open('dags/include/inputs/config_'+company.lower()+'.yml', 'w') as yaml_file:
            yaml.dump(config, yaml_file, default_flow_style=False)

finally:
    cursor.close()
connection.close()