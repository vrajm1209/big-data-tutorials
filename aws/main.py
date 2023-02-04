import os
import boto3
import logging
from dotenv import load_dotenv

#load env variables and change logging level to info
load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

#authenticate S3 client with your user credentials that are stored in your .env config file
s3resource = boto3.resource('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

def list_files_in_user_bucket():
    #logging.debug("fetching objects in user s3 bucket")
    my_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))
    logging.info("Printing files from user S3 bucket")
    for file in my_bucket.objects.all():
        print(file.key)

def list_files_in_goes18_bucket():
    #logging.debug("fetching objects in NOAA s3 bucket")
    user_year = '2022' #replace this with user input from streamlit UI
    user_day = '209' #replace this with user input from streamlit UI
    user_hour = '00' #replace this with user input from streamlit UI
    product_for_prototype = 'ABI-L1b-RadC' #set to a single product as per assignment requirement
    prefix = product_for_prototype+'/'+user_year+'/'+user_day+'/'+user_hour+'/'
    goes18_bucket = s3resource.Bucket(os.environ.get('GOES18_BUCKET_NAME'))
    logging.info("Printing files from NOAA S3 bucket")
    print("Files available to download from the selected location:")
    for objects in goes18_bucket.objects.filter(Prefix=prefix):
        file_path = objects.key
        file_path = file_path.split('/')
        print(file_path[-1])

    selected_file_name = 'OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    #selected_file_name = 'OR_ABI-L1b-RadC-M6C01_G18_s20222090011140_e20222090013513_c20222090013556.nc'
    all_selections_string = prefix+selected_file_name
    #print('Selected values with file name: ', all_selections_string)
    return all_selections_string, selected_file_name

def list_files_in_nexrad_bucket():
    #logging.debug("fetching objects in NOAA s3 bucket")
    user_year = '2023' #set to a single year as per assignment requirement
    user_month = '01'
    user_day = '01' #replace this with user input from streamlit UI
    user_ground_station = 'KABR' #replace this with user input from streamlit UI
    prefix = user_year+'/'+user_month+'/'+user_day+'/'+user_ground_station+'/'
    nexrad_bucket = s3resource.Bucket(os.environ.get('NEXRAD_BUCKET_NAME'))
    logging.info("Printing files from NOAA S3 bucket")
    print("Files available to download from the selected location:")
    for objects in nexrad_bucket.objects.filter(Prefix=prefix):
        file_path = objects.key
        file_path = file_path.split('/')
        print(file_path[-1])

    selected_file_name = 'KABR20230101_005634_V06_MDM'
    #selected_file_name = 'KABR20230101_000142_V06'
    all_selections_string = prefix+selected_file_name
    #print('Selected values with file name: ', all_selections_string)
    return all_selections_string, selected_file_name

def copy_file_to_user_bucket(all_selections_string, selected_file_name):
    logging.info("Copying selected file to local S3 bucket")
    destination_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))
    select_satellite = 'nexrad' #replace this with user input from streamlit UI
    
    if(select_satellite == 'goes18'):
        destination_folder = 'goes18/'
        destination_key = destination_folder + selected_file_name
        url_to_mys3 = 'https://sevir-bucket-01.s3.amazonaws.com/' + destination_key
        url_to_noaa = 'https://noaa-goes18.s3.amazonaws.com/' + all_selections_string
        copy_source = {
            'Bucket': os.environ.get('GOES18_BUCKET_NAME'),
            'Key': all_selections_string
            }

    elif(select_satellite == 'nexrad'):
        destination_folder = 'nexrad/'
        destination_key = destination_folder + selected_file_name
        url_to_mys3 = 'https://sevir-bucket-01.s3.amazonaws.com/' + destination_key
        url_to_noaa = 'https://noaa-nexrad-level2.s3.amazonaws.com/' + all_selections_string
        copy_source = {
            'Bucket': os.environ.get('NEXRAD_BUCKET_NAME'),
            'Key': all_selections_string
            }

    for file in destination_bucket.objects.all():
        if(file.key == destination_key):
            print('Sorry! Cannot copy a file that is already present')
            print('DOWNLOAD here using URL to already existing file on local S3 bucket: ')
            print(url_to_mys3)
            print('For reference, here is a link to the original file source: ', url_to_noaa)
            logging.info("Exited due to existing duplicate")
            return url_to_mys3, url_to_noaa

    destination_bucket.copy(copy_source, destination_key)
    logging.info("File copied to S3 bucket successfully")
    #url_to_noaa = 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/' + '2022/209/00/' + 'OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    print('DOWNLOAD file: ')
    print(url_to_mys3)
    print('For reference, here is a link to the original file source: ', url_to_noaa)
    return url_to_mys3, url_to_noaa

def main():
    #list_files_in_user_bucket()
    #goes18_selections_string, goes18_selected_file_name = list_files_in_goes18_bucket()
    nexrad_selections_string, nexrad_selected_file_name = list_files_in_nexrad_bucket()
    #url_to_mys3, url_to_noaa = copy_file_to_user_bucket(goes18_selections_string, goes18_selected_file_name)
    url_to_mys3, url_to_noaa = copy_file_to_user_bucket(nexrad_selections_string, nexrad_selected_file_name)

if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")