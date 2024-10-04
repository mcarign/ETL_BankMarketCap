import pandas as pd 
from bs4 import BeautifulSoup
import sqlite3
import numpy as np 
import requests
from datetime import datetime

# Run this in terminal before running python code
# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
excahnge_rate_csv = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
out_csv = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

def log_progress(message):
    timestamp_format = '%Y-%h-%D-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(f'./{log_file}', 'a') as f:
        f.write(f'{timestamp} : {message}\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_entry = {f'{table_attribs[0]}' : col[1].contents[2].text,
                        f'{table_attribs[1]}' : float(col[2].contents[0].replace('\n',''))}
            df1 = pd.DataFrame(data_entry, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    with open('./exchange_rate.csv', 'r') as file:
        data = pd.read_csv(file)
        exchange_rate_dict = data.set_index('Currency').to_dict()['Rate']
    other_country_columns = ['MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
    other_country_df = pd.DataFrame(columns=other_country_columns)
    df = pd.concat([df, other_country_df], ignore_index=True)

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, excahnge_rate_csv)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, out_csv)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

# SQL queries
query_statement = f'SELECT * FROM {table_name}'
run_queries(query_statement, conn)
log_progress('Process Complete')

query_statement = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_queries(query_statement, conn)
log_progress('Process Complete')

query_statement = f'SELECT Name from {table_name} LIMIT 5'
run_queries(query_statement, conn)
log_progress('Process Complete')

conn.close()
log_progress('Server Connection closed')