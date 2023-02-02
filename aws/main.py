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
    prefix = 'ABI-L1b-RadC/2022/209/00/' #replace this with user input from streamlit UI with / in end
    goes18_bucket = s3resource.Bucket(os.environ.get('GOES18_BUCKET_NAME'))
    logging.info("Printing files from NOAA S3 bucket")
    for objects in goes18_bucket.objects.filter(Prefix=prefix):
        print(objects.key)

def copy_file_to_user_bucket():
    logging.info("Copying selected file to local S3 bucket")
    destination_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))
    destination_folder = "goes18/"
    destination_key = destination_folder + 'OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    url_to_mys3 = 'https://sevir-bucket-01.s3.amazonaws.com/' + destination_key
    for file in destination_bucket.objects.all():
        if(file.key == destination_key):
            print('Sorry! Can not copy a file that is already present')
            print('URL to already existing file on local S3 bucker: ', url_to_mys3)
            logging.info("Exited due to existing duplicate")
            return
    copy_source = {
        'Bucket': os.environ.get('GOES18_BUCKET_NAME'),
        'Key': 'ABI-L1b-RadC/2022/209/00/OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    }
    destination_bucket.copy(copy_source, destination_key)
    url_to_noaa = 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/' + '2022/209/00/' + 'OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    print('URL to corresponding file on NOAA GOES18 S3 bucket: ', url_to_noaa)
    print('URL to file copied to local S3 bucket: ', url_to_mys3)

def main():
    list_files_in_user_bucket()
    list_files_in_goes18_bucket()
    copy_file_to_user_bucket()

if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")