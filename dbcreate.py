#importing the libraries
import pandas as pd
from sqlalchemy import create_engine
import os

# MySQL connection parameters
#Connect to the database
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# Create a MySQL database connection
db_url = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# Define the file paths for your Excel sheets
status_file = "C:\\Users\\VIWIN\\Downloads\\store status.csv"
business_hours_file = "C:\\Users\\VIWIN\Downloads\\Menu hours.csv"
timezones_file = "C:\\Users\\VIWIN\Downloads\\bq-results-20230125-202210-1674678181880.csv"


# Read CSV files into DataFrames
df_status = pd.read_csv(status_file)
df_business_hours = pd.read_csv(business_hours_file)
df_timezones = pd.read_csv(timezones_file)

# Insert data into MySQL tables
df_status.to_sql('status_data', con=engine, if_exists='replace', index=False)
df_business_hours.to_sql('business_hours', con=engine, if_exists='replace', index=False)
df_timezones.to_sql('timezones', con=engine, if_exists='replace', index=False)

print("Data inserted into MySQL tables successfully.")
