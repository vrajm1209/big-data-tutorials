import os
import logging
import sqlite3
import pandas as pd
from pathlib import Path
from scraper_goes18 import scrape_goes18_data
from scraper_nexrad import scrape_nexrad_data
from scraper_mapdata import scrape_nexrad_locations

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')

database_file_name = 'assignment1.db'
database_file_path = os.path.join(os.path.dirname(__file__),database_file_name)
print("File path", database_file_path)

ddl_file_name = 'nexrad.sql'
ddl_file_path = os.path.join(os.path.dirname(__file__),ddl_file_name)
#data_nexrad = pd.read_csv("scrapped_nexrad_df.csv")
data_nexrad = scrape_nexrad_data()
table_name = 'nexrad'

def create_database():
    with open(ddl_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
    db = sqlite3.connect(database_file_path)
    data_nexrad.to_sql(table_name,db,if_exists='replace',index=False) 

    cursor = db.cursor()
    cursor.executescript(sql_script)
    db.commit()
    db.close()

def check_database_initilization():
    print(os.path.dirname(__file__))
    if not Path(database_file_path).is_file():
        logging.info(f"Database file not found, initilizing at : {database_file_path}")
        create_database()
    else:
        logging.info("Database file already exist")

def query_into_dataframe():
    db = sqlite3.connect(database_file_path)
    df_query = pd.read_sql_query("SELECT * FROM nexrad", db)
    #logging.info(df_query)
    print(df_query)

def main():
    check_database_initilization()
    query_into_dataframe()

if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")