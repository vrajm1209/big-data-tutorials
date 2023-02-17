import os
import sqlite3
import pandas as pd
from pathlib import Path

database_file_name = 'sql_scraped_database.db'
database_file_path = os.path.join(os.path.dirname(__file__),database_file_name)

#query product from goes18 data
def get_product_goes():
     db_conn = sqlite3.connect(database_file_path)
     query = "SELECT DISTINCT product FROM GOES_METADATA"
     df_product = pd.read_sql_query(query, db_conn)
     products = df_product['product'].tolist()
     return products
     
#query all the years under the selected product for goes18 data
def get_years_in_product_goes(selected_product):
     db_conn = sqlite3.connect(database_file_path)
     query = "SELECT DISTINCT year FROM GOES_METADATA WHERE product = " + selected_product
     df_year = pd.read_sql_query(query, db_conn)
     years = df_year['year'].tolist()
     return years

#query all the days under the selected year from goes18 data
def get_days_in_year_goes(selected_year, selected_product):
    db_conn = sqlite3.connect(database_file_path)
    query = "SELECT DISTINCT day FROM GOES_METADATA WHERE product = " + selected_year + "AND product = " + selected_product
    df_day = pd.read_sql_query(query, db_conn)
    days = df_day['day'].tolist()
    return days

#query for hours in a selected day from goes18 data
def get_hours_in_day_goes(selected_day,selected_year,selected_product):
      db_conn = sqlite3.connect(database_file_path)
      query = "SELECT DISTINCT hour FROM GOES_METADATA WHERE day = " + selected_day + "AND year = " + selected_year + "AND product = " + selected_product
      df_hour = pd.read_sql_query(query, db_conn)
      hours = df_hour['day'].tolist() 
      return hours

#query year from nexrad
def get_years_nexrad():
     db_conn = sqlite3.connect(database_file_path)
     query = "SELECT DISTINCT year FROM MAPDATA_NEXRAD " 
     df_year = pd.read_sql_query(query, db_conn)
     years = df_year['year'].tolist()
     return years

#query months from selected year from  nexrad
def get_months_in_year_nexrad(selected_year):
    db_conn = sqlite3.connect(database_file_path)
    query = "SELECT DISTINCT month FROM MAPDATA_NEXRAD  WHERE year = " + selected_year
    df_month = pd.read_sql_query(query, db_conn)
    months = df_month['month'].tolist()
    return months

#query days recorded in a selected month from  nexrad
def get_days_in_month_nexrad(selected_month, selected_year):
     db_conn = sqlite3.connect(database_file_path)
     query = "SELECT DISTINCT day FROM MAPDATA_NEXRAD WHERE month = " + selected_month + "AND year = " + selected_year
     df_day = pd.read_sql_query(query, db_conn)
     days = df_day['month'].tolist()
     return days

#query ground stations for a selected day from  nexrad
def get_stations_for_day_nexrad(selected_day, selected_month, selected_year):
      db_conn = sqlite3.connect(database_file_path)
      query = "SELECT DISTINCT ground_station FROM MAPDATA_NEXRAD WHERE day = " + selected_day + "AND month = " + selected_month + " AND year =" + selected_year
      df_station = pd.read_sql_query(query, db_conn)
      stations = df_station['ground_station'].tolist() 
      return stations




