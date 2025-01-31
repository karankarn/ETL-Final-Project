import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3





""" 
-------------
LOG PROGRESS
-------------

Writing a function that logs the progress as the code steps through the script.
The function will take in a message as an argument and write it as a string
along with the time stamp into a .txt file that can be reviewed. 
"""
# global variable that is use in log_progress function
log_file = "log_file.txt"

def log_progress(message):
    """
    the log_progress function stores the format for the required datetime,
    gets the current timestamp using the datetime.now() method.
    the timestamp is formatted as stated previously and written
    to the log_file.
    """
    timeFormat = "%Y-%h-%d-%H:%M:%S" #Year-MonthName-Day-Hour:Minute:Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timeFormat)
    with open(log_file, "a") as file: # 'a' appends the file without overwriting.
        file.write(timestamp + ":" + message + "\n")
        

"""
-------
EXTRACT
-------

extract 
input : url, table index
return : item
"""

def extract(URL, index):
    webdata = pd.read_html(URL, header = 0  )
    data = webdata[index]
    return data

"""
---------
TRANSFORM
---------

Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, 
rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
"""


"""
transform
input : 1. url_data ( from extract function ) 
        2. exchange info table from csv

output : df containing new columns with exchange rate information rounded to 2 decimal places
"""
def transform(url_Data, exchangeInfo):
    # create list a of new column names
    a = ["MC_EUR_Billion","MC_GBP_Billion","MC_INR_Billion"]
    # create df
    df = url_data
    # create columns for exchange info and fill up the values rounded to 2 decimal places
    for i in range(len(a)):
        df[a[i]] = (df["Market cap (US$ billion)"] * exchange_info["Rate"].iloc[i]).round(2)

    return df



"""
----
LOAD
----

create_csv
input : 1. target file ( name of file to save to )
        2. transformed data ( data to be saved )
        
output : creates a csv named by target file
"""

# create target file
target_file = "Biggest_Banks.csv"
# define function
def load_to_csv(target_file, transformed_data):
    transformed_data.to_csv(target_file)

"""
create_db
"""
# connection to database
conn = sqlite3.connect("Banks.db")
# table name
table_name = "Largest_banks"

def load_to_db(dataframe, table_name, conn):
    # convert the df to an sql database
    dataframe.to_sql(table_name,conn,if_exists = "replace", index = False)

"""
-----
QUERY
-----
run_query
inputs : query statement, sql_connection
output : print on terminal
"""


def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)


"""
Running the program
"""
log_progress("Preliminaries complete. Initiating ETL process")
# URL with table I require.
url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"

# extract function called and saved to url_data.
# the required index for the table within url is 1
log_progress("Data Extraction Started")
url_data = extract(url,1)
log_progress("Data extraction complete. Initiating Transformation process")

# reading the csv file containing the exchange info
csv = "D://Projects//Coursera//Codebases//Final Project//Data//exchange_rate.csv"
# saving contents to exchange_info
exchange_info = pd.read_csv(csv)

# transform function called to create dataframe and saved to final
log_progress("Transform data Started")
final = transform(url_data,exchange_info)
final = final.rename(columns={"Bank name": "Name"})
log_progress("Data transformation complete. Initiating Loading process")

# create_csv function called to save final to target_file
log_progress("Create CSV Started")
load_to_csv(target_file, final)
log_progress("Data saved to CSV file")

# create database
log_progress("SQL Connection initiated")
load_to_db(final, table_name,conn)
log_progress("Data loaded to Database as a table, Executing queries")

# run query
query_statement = f'SELECT * FROM "Banks"'
log_progress("Query Started")
run_query(query_statement,conn)
log_progress("Query Completed")


#####
query_statement = f'SELECT * FROM Largest_banks'
run_query(query_statement, conn)
query_statement = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, conn)
query_statement = f'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement, conn)







conn.close()
log_progress("Server Connection closed")
