import os
import boto3
import logging
from dotenv import load_dotenv

load_dotenv()

s3resource = boto3.resource('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

def list_files_in_user_bucket():
    #logging.debug("fetching objects in user s3 bucket")
    my_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))
    for file in my_bucket.objects.all():
        print(file.key)

def list_files_in_noaa_bucket():
    #logging.debug("fetching objects in NOAA s3 bucket")
    #paginator = s3client.get_paginator('list_objects_v2')
    prefix = 'ABI-L1b-RadC/2022/209/00/' #replace this with user input with / in end
    noaa_bucket = s3resource.Bucket('noaa-goes18')
    for objects in noaa_bucket.objects.filter(Prefix=prefix):
        print(objects.key)

def copy_file_to_user_bucket():
    #file_key = prefix + filename 
    copy_source = {
        'Bucket':'noaa-goes18',
        'Key': 'ABI-L1b-RadC/2022/209/00/OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    }
    destination_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))
    destination_key = 'copied/mycopy.nc'
    destination_bucket.copy(copy_source, destination_key)
    url_to_mys3 = 'https://damg7245-tutorial.s3.amazonaws.com/' + 'copied/mycopy.nc'
    url_to_noaa = 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/' + '2022/209/00/' + 'OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    print('URL to object on local S3: ', url_to_mys3)
    print('URL to object on NOAA S3: ', url_to_noaa)

def main():
    #print('hello')
    #return
    list_files_in_user_bucket()
    list_files_in_noaa_bucket()
    copy_file_to_user_bucket()


if __name__ == "__main__":
    #logging.info("Script starts")
    main()
    #logging.info("Script ends")