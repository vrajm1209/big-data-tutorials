import os
import boto3
import logging
from dotenv import load_dotenv

load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY')
aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
aws_s3_bucket = os.environ.get('USER_BUCKET_NAME')
noaa_public_bucket = 'noaa-goes18'
nexrad_public_bucket = ''

s3resource = boto3.resource('s3',
                        region_name='us-east-1',
                        aws_access_key_id = aws_access_key_id,
                        aws_secret_access_key = aws_secret_access_key
                        )

def list_files_in_user_bucket():
    #logging.debug("fetching objects in user s3 bucket")
    my_bucket = s3resource.Bucket(aws_s3_bucket)
    logging.info("Printing files from user S3 bucket")
    for file in my_bucket.objects.all():
        print(file.key)

def list_files_in_noaa_bucket():
    #logging.debug("fetching objects in NOAA s3 bucket")
    prefix = 'ABI-L1b-RadC/2022/209/00/' #replace this with user input with / in end
    noaa_bucket = s3resource.Bucket(noaa_public_bucket)
    logging.info("Printing files from NOAA S3 bucket")
    for objects in noaa_bucket.objects.filter(Prefix=prefix):
        print(objects.key)

def copy_file_to_user_bucket():
    logging.info("Copying selected file to local S3 bucket")
    #file_key = prefix + filename 
    my_bucket = s3resource.Bucket(aws_s3_bucket)
    destination_bucket = s3resource.Bucket(aws_s3_bucket)
    destination_key = 'copied/OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    url_to_mys3 = 'https://damg7245-tutorial.s3.amazonaws.com/' + destination_key
    for file in my_bucket.objects.all():
        if(file.key == destination_key):
            print('Can not copy duplicate')
            logging.info("Exited due to existing duplicate")
            return
    copy_source = {
        'Bucket': noaa_public_bucket,
        'Key': 'ABI-L1b-RadC/2022/209/00/OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    }
    destination_bucket.copy(copy_source, destination_key)
    url_to_noaa = 'https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/' + '2022/209/00/' + 'OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc'
    #print('URL to object on local S3: ', url_to_mys3)
    print('URL to object on NOAA S3: ', url_to_noaa)
    print('URL to object on local S3: ', url_to_mys3)

def main():
    list_files_in_user_bucket()
    list_files_in_noaa_bucket()
    copy_file_to_user_bucket()


if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")