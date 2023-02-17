import os
import sqlite3
import boto3
import time
import pandas as pd
from pathlib import Path
from scraper_goes18 import scrape_goes18_data
from scraper_nexrad import scrape_nexrad_data
from scraper_mapdata import scrape_nexrad_locations
from dotenv import load_dotenv

#load env variables and change logging level to info
load_dotenv()

#authenticate S3 client for logging with your user credentials that are stored in your .env config file
clientLogs = boto3.client('logs',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_LOG_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_LOG_SECRET_KEY')
                        )

def store_scraped_data_to_db(scraped_data, database_file_name, ddl_file_name, table_name):

    """Used to store/load scraped data into a SQLite table within a database. A database file is created if does not exists and then
    the SQL script is run to create a table. Records/data from the input dataframe are then populated into the table.
    -----
    Input parameters:
    scraped_data : DataFrame
        dataframe containing the data you wish to populate into SQLite table
    database_file_name : str
        name of database file along with .db extension for creating SQLite database
    ddl_file_name : str
        name of sql script with .sql extension that contains the create table SQL statement
    table_name : str
        name of the table you wish to enter records into within the database_file_name
    -----
    Returns:
    Nothing 
    """

    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Storing metadate into table: " + table_name + " in " + database_file_name + " database"
            }
        ]
    )
    database_file_path = os.path.join(os.path.dirname(__file__),database_file_name)
    ddl_file_path = os.path.join(os.path.dirname(__file__),ddl_file_name)
    #first check if the database file exists or not
    if not Path(database_file_path).is_file():  #if .db does not exist, create one
        clientLogs.put_log_events(      #logging to AWS CloudWatch logs
            logGroupName = "assignment01-logs",
            logStreamName = "db-logs",
            logEvents = [
                {
                'timestamp' : int(time.time() * 1e3),
                'message' : "Database file not found, initilizing at: " + database_file_path + " and storing data into specified table"
                }
            ]
        )
        #create database
        with open(ddl_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        db_conn = sqlite3.connect(database_file_path)   #connect to the database
        cursor = db_conn.cursor()
        cursor.executescript(sql_script)    #execute the sql script to create the table
        scraped_data.to_sql(table_name, db_conn, if_exists='replace', index=False)     #store scraped data into table and replace table if table already exists
    
    else:   #if database already exists
        clientLogs.put_log_events(      #logging to AWS CloudWatch logs
            logGroupName = "assignment01-logs",
            logStreamName = "db-logs",
            logEvents = [
                {
                'timestamp' : int(time.time() * 1e3),
                'message' : "Database file found, saving data into table"
                }
            ]
        )
        db_conn = sqlite3.connect(database_file_path)   #connect to the database
        cursor = db_conn.cursor()
        scraped_data.to_sql(table_name, db_conn, if_exists='replace', index=False)     #store scraped data into table and replace table if table already exists

    db_conn.commit()
    db_conn.close()     #finally commit changes to database and close

def main():
    #call scraper function to perform data scraping for all sources required
    scraped_goes18_df = scrape_goes18_data()   #scrape goes18 bucket data and store result as dataframe
    scraped_nexrad_df = scrape_nexrad_data()   #scrape nexrad bucket data and store result as dataframe
    scraped_map_df = pd.DataFrame(scrape_nexrad_locations())   #scrape nexrad map data and store result as dataframe
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "All scraping scripts ran successfully"
            }
        ]
    )

    #next, store this scraped data into a database; define variables for database name, individual sql scripts and individual table names
    database_file_name = 'sql_scraped_database.db'
    goes_ddl_file_name = 'sql_script_goes18.sql'
    goes_table_name = 'GOES_METADATA'
    nexrad_ddl_file_name = 'sql_script_nexrad.sql'
    nexrad_table_name = 'NEXRAD_METADATA'
    map_ddl_file_name = 'sql_script_mapdata.sql'
    map_table_name = 'MAPDATA_NEXRAD'

    #call function to move scraped data into SQLite database for all metadata that was extracted above
    store_scraped_data_to_db(scraped_goes18_df, database_file_name, goes_ddl_file_name, goes_table_name)
    store_scraped_data_to_db(scraped_nexrad_df, database_file_name, nexrad_ddl_file_name, nexrad_table_name)
    store_scraped_data_to_db(scraped_map_df, database_file_name, map_ddl_file_name, map_table_name)
    
    #the following lines have been commented out since they were needed to run once only, to generate the csv file that has been used in our GreatExpectations part
    #db = sqlite3.connect(os.path.join(os.path.dirname(__file__),database_file_name))
    #df_goes = pd.read_sql_query("SELECT * FROM GOES_METADATA", db)
    #df_goes.to_csv("goes18_db_extract.csv")
    #df_nexrad = pd.read_sql_query("SELECT * FROM NEXRAD_METADATA", db)
    #df_nexrad.to_csv("nexrad_db_extract.csv")

    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "All scraped data stored into database successfully"
            }
        ]
    )

if __name__ == "__main__":
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Scrape & save in database script starts"
            }
        ]
    )
    main()
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Scrape & save in database script ends"
            }
        ]
    )