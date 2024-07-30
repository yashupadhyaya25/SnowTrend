import pandas as pd
from sqlalchemy import create_engine

# Database credentials
USER = 'username'
PASSWORD = 'password'
HOST = 'hostname'
PORT = '3306'
DATABASE = 'database'

# Create a connection engine to the MySQL database
engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

# Read the CSV file into a DataFrame
csv_file_path = 'your_file.csv'
df = pd.read_csv(csv_file_path)

# Define the target table name
table_name = 'your_table'

# Upload the DataFrame to MySQL
df.to_sql(table_name, con=engine, if_exists='replace', index=False)

print(f"Data successfully uploaded to table '{table_name}' in the database.")
