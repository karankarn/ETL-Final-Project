import pandas as pd
import numpy as np
from datetime import datetime

""" 
Writing a function that logs the progress as the code steps through the script.
The function will take in a message as an arguement and write it as a string
along with the time stamp into a .txt file that can be reviewed. 
"""
# global variable that is use in log_progress function
log_file = "log_file.txt"

def log_progress(message):
    '''
    the log_progress funtion stores the format for the required datetime,
    gets the current timestamp using the datetime.now() method.
    the timestamp is formatted as stated previously and written
    to the log_file. 
    '''
    format = "%Y-%h-%d-%H:%M:%S" #Year-MonthName-Day-Hour:Minute:Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(format)
    with open(log_file, "a") as file: # 'a' appends the file without overwriting.
        file.write(timestamp + "," + message + "\n")
        

"""
extract 
input : url, table index
return : item
"""

def extract(URL, index):
    webdata = pd.read_html(URL, header = 0)
    data = webdata[index]
    return data   


# THE INDEX POSITION IS 1 FOR THE TABLE I REQUIRE  
url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
url_data = extract(url,1)  
csv = "D://Projects//Coursera//Codebases//Final Project//Data//exchange_rate.csv"
exchange_info = pd.read_csv(csv)

"""
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, 
rounded to 2 decimal places, based on the exchange rate information shared as a CSV file."""

EUR = exchange_info.loc[exchange_info["Currency"] == "EUR", "Rate"].values[0]
GBP = exchange_info.loc[exchange_info["Currency"] == "GBP", "Rate"].values[0]
INR = exchange_info.loc[exchange_info["Currency"] == "INR", "Rate"].values[0]

df = url_data
columns = ["Rank","BankName","MarketCap","EUR_x","GBP_x","INR_x"]

df = df.assign(EUR_x=df["Market cap (US$ billion)"] * EUR)
df = df.assign(GBP_x=df["Market cap (US$ billion)"] * GBP)
df = df.assign(INR_x=df["Market cap (US$ billion)"] * INR)



