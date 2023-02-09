import streamlit as st
import boto3
import re
import scraper_mapdata
import pandas as pd
#import logging
import os
import time
from dotenv import load_dotenv
from scraper_goes18 import scrape_goes18_data
from transfer_files import list_files_in_goes18_bucket, copy_goes_file_to_user_bucket
from filename import generate_goes_url

#load env variables and change logging level to info
load_dotenv()

#change logging level to info
#LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
#logging.basicConfig(
#    format='%(asctime)s %(levelname)-8s %(message)s',
#    level=LOGLEVEL,
#    datefmt='%Y-%m-%d %H:%M:%S',
#    filename='logs.log')

#initialize the S3 client
#s3 = boto3.client("s3")

clientLogs = boto3.client('logs',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_LOG_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_LOG_SECRET_KEY')
                        )

def goes_main():
    #st.set_page_config(page_title="GOES Satellite Sites", page_icon=":satellite:", layout="wide")
    st.title("GOES-18 Satellite File Downloader")
    st.markdown(
        """
        <style>
            .title {
                text-align: center;
                color: #2F80ED;
            }
        </style>
        <h2 class="title">Find the latest GOES Radar Data</h2>
        <p></p>
        """,
        unsafe_allow_html=True,
    )

    #search options
    download_option = st.sidebar.radio ("Use following to search for GOES radar data:",['Search by entering fields', 'Search by filename'])
    # add while loop to display something until none of the radio button/checkboxes are selected? 

    #search by fields
    if (download_option == "Search by entering fields"):
        st.write("Select all options in this form to download ")
        #bring in metadata from database to populate fields
        goes18_data = scrape_goes18_data()
        #product select box has a pre-selected value as per scope of assignment
        product_box = st.selectbox("Product name: ", goes18_data['product'].unique().tolist(), disabled = True, key="selected_product")
        #define year box
        year_box = st.selectbox("Year for which you are looking to get data for: ", ["--"]+goes18_data['year'].unique().tolist(), key="selected_year")
        if (year_box == "--"):
            st.warning("Please select an year!")
        else:
            days_in_selected_year = goes18_data.loc[goes18_data['year']==year_box]['day'].unique().tolist()     #days in selected year
            #define day box
            day_box = st.selectbox("Day within year for which you want data: ", ["--"]+days_in_selected_year, key="selected_day")
            if (day_box == "--"):
                st.warning("Please select a day!")
            else:
                hours_in_selected_day = goes18_data.loc[goes18_data['day']==day_box]['hour'].unique().tolist()      #hours in selected day     
                #define hour box
                hour_box = st.selectbox("Hour of the day for which you want data: ", ["--"]+hours_in_selected_day, key='selected_hour')
                if (hour_box == "--"):
                    st.warning("Please select an hour!")
                else: 
                    #display selections
                    st.write("Current selections, Product: ", product_box, ", Year: ", year_box, ", Day: ", day_box, ", Hour: ", hour_box)

                    #execute function with spinner
                    with st.spinner("Loading..."):
                        files_in_selected_hour = list_files_in_goes18_bucket(product_box, year_box, day_box, hour_box)      #list file names for given selection

                    if files_in_selected_hour:
                        #define file box
                        file_box = st.selectbox("Select a file: ", files_in_selected_hour, key='selected_file')
                        if file_box:
                            with st.spinner("Loading..."):
                                download_url = copy_goes_file_to_user_bucket(file_box, product_box, year_box, day_box, hour_box)    #copy the selected file into user bucket
                            if (download_url):
                                st.success("File available for download.")      #display success message
                                if (st.button("Download file")):
                                    st.write("URL to download file:", download_url)     #provide download URL
                    else:
                        st.warning("Something went wrong, no files found.")
  
    #search by filename
    if (download_option == "Search by filename"):
        #filename text box
        filename_entered = st.text_input("Enter the filename")
        #fetch URL while calling spinner element
        with st.spinner("Loading..."):
            final_url = generate_goes_url(filename_entered)     #call relevant function
        #file = goes_fetch_file_by_filename(filename)

        if (final_url == -1):
            st.warning("No such file exists at GOES18 location")    #display no such file exists message
        elif (final_url == 1):
            st.error("Invalid filename format for GOES18")      #display invalid filename message
        else: 
            st.success("Link of the file available on GOES bucket:", final_url)     #display success message
        
def nexrad_main():
    #st.set_page_config(page_title="NEXRAD Doppler Radar Sites", page_icon=":radar_dish:", layout="wide")
    st.title("NEXRAD Radar File Downloader")
    st.markdown(
        """
        <style>
            .title {
                text-align: center;
                color: #2F80ED;
            }
        </style>
        <h2 class="title">Find the latest NEXRAD Radar Data</h2>
        <p></p>
        """,
        unsafe_allow_html=True,
    )

    #search options
    download_option = st.sidebar.radio ("Use following to search for NEXRAD radar data:",['Search by entering fields', 'Search by filename'])

    #search by fields
    if (download_option == "Search by entering fields"):
        st.write("Select all options in this form to download ")
        #bring in metadata from database to populate fields
        goes18_data = scrape_goes18_data()
        #product select box has a pre-selected value as per scope of assignment
        product_box = st.selectbox("Product name: ", goes18_data['product'].unique().tolist(), disabled = True, key="selected_product")
        #define year box
        year_box = st.selectbox("Year for which you are looking to get data for: ", ["--"]+goes18_data['year'].unique().tolist(), key="selected_year")
        if (year_box == "--"):
            st.warning("Please select an year!")
        else:
            days_in_selected_year = goes18_data.loc[goes18_data['year']==year_box]['day'].unique().tolist()     #days in selected year
            #define day box
            day_box = st.selectbox("Day within year for which you want data: ", ["--"]+days_in_selected_year, key="selected_day")
            if (day_box == "--"):
                st.warning("Please select a day!")
            else:
                hours_in_selected_day = goes18_data.loc[goes18_data['day']==day_box]['hour'].unique().tolist()      #hours in selected day     
                #define hour box
                hour_box = st.selectbox("Hour of the day for which you want data: ", ["--"]+hours_in_selected_day, key='selected_hour')
                if (hour_box == "--"):
                    st.warning("Please select an hour!")
                else: 
                    #display selections
                    st.write("Current selections, Product: ", product_box, ", Year: ", year_box, ", Day: ", day_box, ", Hour: ", hour_box)

                    #execute function with spinner
                    with st.spinner("Loading..."):
                        files_in_selected_hour = list_files_in_goes18_bucket(product_box, year_box, day_box, hour_box)      #list file names for given selection

                    if files_in_selected_hour:
                        #define file box
                        file_box = st.selectbox("Select a file: ", files_in_selected_hour, key='selected_file')
                        if file_box:
                            with st.spinner("Loading..."):
                                download_url = copy_goes_file_to_user_bucket(file_box, product_box, year_box, day_box, hour_box)    #copy the selected file into user bucket
                            if (download_url):
                                st.success("File available for download.")      #display success message
                                if (st.button("Download file")):
                                    st.write("URL to download file:", download_url)     #provide download URL
                    else:
                        st.warning("Something went wrong, no files found.")

    #search by filename
    if search_by_filename:
        filename = st.text_input("Enter the filename")

        with st.spinner("Loading..."):
            file = fetch_file_by_filename(filename)

        if file:
            st.write("Link of the file available on NEXRAD bucket:", file)

def map_main():
    #st.title("Map Page")
    st.markdown(
        """
        <h1 style="background-color:#1c1c1c; color: white; text-align: center; padding: 15px; border-radius: 10px">
            Map Page
        </h1>
        """,
        unsafe_allow_html=True,
    )

    with st.spinner("Loading..."):
        map_data = scraper_mapdata.plot_nexrad_locations()      #calling relevant function to generat chart
    st.plotly_chart(map_data, use_container_width=True, height=700)     #plotting on streamlit page

def main():
    st.set_page_config(page_title="Weather Data Files", layout="wide")
    page = st.sidebar.selectbox("Select a page", ["GOES-18", "NEXRAD", "NEXRAD Locations - Map"])   #main options of streamlit app

    if page == "GOES-18":
        with st.spinner("Loading..."): #spinner element
            goes_main()
    elif page == "NEXRAD":
        with st.spinner("Loading..."): #spinner element
            nexrad_main()
    elif page == "NEXRAD Locations - Map":
        with st.spinner("Generating map..."): #spinner element
            map_main()

if __name__ == "__main__":
    #logging.info("Application script starts")
    main()
    #logging.info("Application script ends")