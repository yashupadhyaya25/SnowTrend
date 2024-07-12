import snowflake.connector as snow
import yaml
from configparser import ConfigParser

config = ConfigParser()
config.read('airflow/airflow_config.ini')
# Create a connection to Snowflake
connection = snow.connect(
    account=config.get('DEV','account'),
    user=config.get('DEV','user'),
    password=config.get('DEV','password')
)

# Define a cursor
cursor = connection.cursor()

try:
    # select from table where your all yahoo config store
    cursor.execute("SELECT * FROM RAW.CONFIG.YAHOO_CONF where IS_ACTIVE <> FALSE") 
    result_set = cursor.fetchall()
    for result in result_set :
        company = result[1].split('.')[0]
        config = {'dag_id': company,
              'schedule_interval' : '@daily',
              'catchup' : False,
              'owner' : 'Yash',
              'database' : 'RAW',
              'schema' : 'STOCK',
              'table' : company,
              'ticker' : result[1]
              }
        with open('airflow/dags/include/inputs/config_'+company.lower()+'.yml', 'w') as yaml_file:
            yaml.dump(config, yaml_file, default_flow_style=False)

finally:
    cursor.close()
connection.close()