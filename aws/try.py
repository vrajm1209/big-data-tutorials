import streamlit as st
import boto3
import re
import scraper_mapdata
import pandas as pd
import logging
import os
from scraper_goes18 import scrape_goes18_data
from transfer_files import list_files_in_goes18_bucket, copy_goes_file_to_user_bucket
from filename import generate_goes_url
import time 

#change logging level to info
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

#initialize the S3 client
s3 = boto3.client("s3")

states = ["AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", 
"IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS",
 "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", 
 "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"]

nexrad_stations = {
"AK": ["PAHC", "PAHO", "PAJK", "PAKN"],
"AL": ["KBMX", "KHTX", "KMXX", "KEOX"],
"AR": ["KLZK", "KLRF", "KSHV"],
"AZ": ["KEMX", "KFDX", "KPSR"],
"CA": ["KDAX", "KHNX", "KMUX"],
"CO": ["KCYS", "KFTG", "KGRB"],
"CT": ["KOKX"],
"DC": ["KLWX"],
"DE": ["KDOX"],
"FL": ["KAMX", "KBYX", "KEYW", "KMLB"],
"GA": ["KFFC", "KJGX"],
"HI": ["PHKI"],
"IA": ["KDMX", "KDVX", "KOAX"],
"ID": ["KBOI", "KIDA", "KPIH"],
"IL": ["KILX", "KLOT"],
"IN": ["KIND", "KLOT"],
"KS": ["KDDC", "KGLD", "KICT"],
"KY": ["KLMK", "KPAH"],
"LA": ["KLCH", "KLIX"],
"MA": ["KBOX"],
"MD": ["KMTN"],
"ME": ["KGYX"],
"MI": ["KGRR", "KDTX"],
"MN": ["KMPX"],
"MO": ["KDZX", "KSGF"],
"MS": ["KJAN", "KMOB"],
"MT": ["KGGW", "KTFX"],
"NC": ["KGSP", "KMHX"],
"ND": ["KBIS", "KFGF"],
"NE": ["KLBF", "KOAX"],
"NH": ["KGYX"],
"NJ": ["KPHI"],
"NM": ["KABQ", "KFMX"],
"NV": ["KREV"],
"NY": ["KALY", "KBGM"],
"OH": ["KCLE", "KILN"],
"OK": ["KFDR", "KOUN"],
"OR": ["KPDT", "KRTX"],
"PA": ["KCCX", "KPBZ"],
"RI": ["KBOX"],
"SC": ["KCAE", "KCHS"],
"SD": ["KABR", "KFSD"],
"TN": ["KHTX", "KMEG"],
"TX": ["KBRO", "KCRP", "KEWX", "KFWD", "KGRK", "KHGX", "KLCH", "KLZK", "KMAF", "KMRX", "KSHV", "KSJT"],
"UT": ["KPUC"],
"VT": ["KBTV"],
"VA": ["KAKQ", "KLWX", "KMHX"],
"WA": ["KATX", "KOTX"],
"WV": ["KRLX"],
"WI": ["KARX", "KGRB", "KMKX"],
"WY": ["KCYS", "KRIW"]
}

def fetch_files_by_fields(product, year, day, hour):
    # Search the files by filtering through the fields

    pass

def fetch_file_by_filename(filename):
    # Search the file by its complete name
    pass

def goes_fetch_file_by_filename(file_name):
    # # Search the file by its complete name
    # pattern = re.compile(r'OR_ABI-L1b-RadC-M\dC\d\d_G\d\d_s\d{15}_e\d{15}_c\d{15}\.nc')
    # if not pattern.match(filename):
    #     raise ValueError("Invalid filename format")
    
    # elements = filename.split("_")
    # year = elements[3][3:7]
    # day_of_year = elements[3][7:10]
    # path = f"ABI-L1b-RadC/{year}/{day_of_year}/{elements[4][0:3]}/"
    # link = f"https://noaa-goes18.s3.amazonaws.com/{path}{filename}"
    
    # return link
    input_url = "https://noaa-goes18.s3.amazonaws.com/"
    file_name = file_name.strip()
    file_list = file_name.split("_")
    sublist=file_list[1].split("-")
    if (sublist[2].isalpha()) is False:
        sublist[2] = sublist[2][:-1]
    sublist_date = file_list[3]

    final_url = input_url+"-".join(sublist[0:3])+'/'+sublist_date[1:5]+'/'+sublist_date[5:8]+'/'+sublist_date[8:10]+'/'+file_name

    return final_url

def copy_file_to_bucket(file_path):
    # Copy the selected file to your S3 bucket
    pass

def retrieve_url_from_bucket(file_path):
    # Retrieve the URL of the file from your S3 bucket
    pass

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
        <h2 class="title">Find the Latest GOES radar Data</h2>
        <p>Use the following options to search for GOES radar data.</p>
        """,
        unsafe_allow_html=True,
    )

    ##search options
    #search_by_fields = st.sidebar.radio ("Select an option:",['Search by entering fields', 'Search by field'])      #use st.radio? 
    #search_by_filename = st.sidebar.radio("Search by filename")
    download_option = st.sidebar.radio ("Use following to search for GOES radar data:",['Search by entering fields', 'Search by filename'])
    # add while loop to display something until none of the radio button/checkboxes are selected? 

    # search by fields
    if (download_option == "Search by entering fields"):
        st.write("Select all options in this form to download ")
        goes18_data = scrape_goes18_data()
        product_box = st.selectbox("Product name: ", goes18_data['product'].unique().tolist(), disabled = True, key="selected_product")
        #station = goes18_data['product'].unique()
        year_box = st.selectbox("Year for which you are looking to get data for: ", ["--"]+goes18_data['year'].unique().tolist(), key="selected_year")
        #month = st.selectbox("Month", range(1,13), key='month')
        if (year_box == "--"):
            st.warning("Please select an year!")
        else:
            days_in_selected_year = goes18_data.loc[goes18_data['year']==year_box]['day'].unique().tolist()
        #if year==2022:
        #    day = st.selectbox("Day for which you are looking to get data for", range(209,366), key='day')
        #elif year==2023:
        #    day = st.selectbox("Day", range(1,33), key='day')
            day_box = st.selectbox("Day within year for which you want data: ", ["--"]+days_in_selected_year, key="selected_day")
            if (day_box == "--"):
                st.warning("Please select a day!")
            else:
                hours_in_selected_day = goes18_data.loc[goes18_data['day']==day_box]['hour'].unique().tolist()      
                hour_box = st.selectbox("Hour of the day for which you want data: ", ["--"]+hours_in_selected_day, key='selected_hour')
                if (hour_box == "--"):
                    st.warning("Please select an hour!")
                else: 
                    #display selections
                    st.write("Current selections, Product: ", product_box, ", Year: ", year_box, ", Day: ", day_box, ", Hour: ", hour_box)

                    #files = fetch_files_by_fields(product_box, year_box, day_box, hour_box)
                    ## new line added for spinner
                    with st.spinner("Loading..."):
                        files_in_selected_hour = list_files_in_goes18_bucket(product_box, year_box, day_box, hour_box)

                    if files_in_selected_hour:
                        file_box = st.selectbox("Select a file: ", files_in_selected_hour, key='selected_file')
                        if file_box:
                            with st.spinner("Loading..."):
                                download_url = copy_goes_file_to_user_bucket(file_box, product_box, year_box, day_box, hour_box)
                            #url = retrieve_url_from_bucket(file_path)
                            if (download_url):
                                st.success("File available for download.")
                                if (st.button("Download file")):
                                    st.write("URL to download file:", download_url)
                            else: 
                                st.write("Something went wrong, unable to download file.")
                    else:
                        st.warning("Something went wrong, no files found.")

    
    #search by filename
    if (download_option == "Search by filename"):
        filename_entered = st.text_input("Enter the filename")

        with st.spinner("Loading..."):
            final_url = generate_goes_url(filename_entered)
        #file = goes_fetch_file_by_filename(filename)

        if (final_url == -1):
            st.warning("No such file exists at GOES18 location")
        elif (final_url == 1):
            st.error("Invalid filename format for GOES18")
        else: 
            st.success("Link of the file available on GOES bucket:", final_url)
        
def nexrad_main():
    #st.set_page_config(page_title="NEXRAD Doppler Radar Sites", page_icon=":radar_dish:", layout="wide")

    st.title("NEXRAD Doppler Radar Sites")
    st.markdown(
        """
        <style>
            .title {
                text-align: center;
                color: #2F80ED;
            }
        </style>
        <h2 class="title">Find the Latest NEXRAD Radar Data</h2>
        <p>Use the following options to search for NEXRAD radar data.</p>
        """,
        unsafe_allow_html=True,
    )

    ##search options
    search_by_fields = st.sidebar.checkbox("Search by Fields")
    search_by_filename = st.sidebar.checkbox("Search by Filename")

    # search by fields
    if search_by_fields:
        year = st.selectbox("Year", [2020, 2021, 2022, 2023], key='year')
        month = st.selectbox("Month", range(1,13), key='month')
        day = st.selectbox("Day", range(1,32), key='day')
        state = st.selectbox("State", states, key='state')
        station = st.selectbox("NEXRAD Station", nexrad_stations[state], key='station')

        #display selections
        st.write("Selected values: Year:", year, ", Month:", month, ", Day:", day, ", State:", state, ", Station:", station)

        with st.spinner("Loading..."):
            files = fetch_files_by_fields(year, month, day, state, station)

        if files:
            file_select = st.selectbox("Select a file", files, key='file')
        else:
            st.warning("No files found.")

        if file_select:
            with st.spinner("Loading..."):
                file_path = copy_file_to_bucket(file_select)
            url = retrieve_url_from_bucket(file_path)
            st.write("URL of the selected file:", url)
            st.success("File copied successfully.")

        # search by filename
    if search_by_filename:
        filename = st.text_input("Enter the filename")

        with st.spinner("Loading..."):
            file = fetch_file_by_filename(filename)

        if file:
            st.write("Link of the file available on NEXRAD bucket:", file)

def map_main():
    # st.title("Map Page")
    st.markdown(
        """
        <h1 style="background-color:#1c1c1c; color: white; text-align: center; padding: 15px; border-radius: 10px">
            Map Page
        </h1>
        """,
        unsafe_allow_html=True,
    )

    with st.spinner("Loading..."):
        map_data = scraper_mapdata.plot_nexrad_locations()
    st.plotly_chart(map_data, use_container_width=True, height=700)

# def main():
#     st.set_page_config(page_title="Weather Data Files", layout="wide")
#     page = st.sidebar.selectbox("Select a page", ["GOES-18", "NEXRAD", "NEXRAD Locations - Map"])

#     if page == "GOES-18":
#         goes_main()
#     elif page == "NEXRAD":
#         nexrad_main()
#     elif page == "NEXRAD Locations - Map":
#         map_main()

def main():
    st.set_page_config(page_title="Weather Data Files", layout="wide")
    page = st.sidebar.selectbox("Select a page", ["GOES-18", "NEXRAD", "NEXRAD Locations - Map"])

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
    logging.info("Application script starts")
    main()
    logging.info("Application script ends")