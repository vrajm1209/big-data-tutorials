import os
import logging
import sqlite3
import pandas as pd
from pathlib import Path

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')

database_file_name = 'assignment1.db'
database_file_path = os.path.join(os.path.dirname(__file__),database_file_name)
print("File path", database_file_path)

ddl_file_name = 'goes18.sql'
ddl_file_path = os.path.join(os.path.dirname(__file__),ddl_file_name)
df = pd.read_csv("scrapped_goes18_df.csv")
table_name = 'goes'

def create_database():
    with open(ddl_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
    db = sqlite3.connect(database_file_path)
    df.to_sql(table_name,db,if_exists='replace',index=False)

    cursor = db.cursor()
    cursor.executescript(sql_script)
    db.commit()
    db.close()


def check_database_initilization():
    print(os.path.dirname(__file__))
    if  Path(database_file_path).is_file():
        logging.info(f"Database exists, initilizing at : {database_file_path}")
        create_database()
    else:
        logging.info("Database file already exist")

def query_into_dataframe():
    db = sqlite3.connect(database_file_path)
    df1 = pd.read_sql_query("SELECT * FROM goes", db)
    logging.info(df1)

def main():
    check_database_initilization()
    query_into_dataframe()

if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")