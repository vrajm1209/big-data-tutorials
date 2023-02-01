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

s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

def scrape_goes18_data():
    logging.debug("Scrapping data from GOES18 bucket")
    prefix = "ABI-L1b-RadC/" #replace this with user input with / in end
    result = s3client.list_objects(Bucket='noaa-goes18', Prefix=prefix, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        path = o.get('Prefix').split('/')
        prefix_2 = prefix + path[-2] + "/"
        sub_folder = s3client.list_objects(Bucket='noaa-goes18', Prefix=prefix_2, Delimiter='/')
        for p in sub_folder.get('CommonPrefixes'):
            sub_path = p.get('Prefix').split('/')
            prefix_3 = prefix_2 + sub_path[-2] + "/"
            sub_sub_folder = s3client.list_objects(Bucket='noaa-goes18', Prefix=prefix_3, Delimiter='/')
            for q in sub_sub_folder.get('CommonPrefixes'):
                sub_sub_path = q.get('Prefix').split('/')
                sub_sub_path = sub_sub_path[:-1]
                #print(sub_sub_path)

def main():
    scrape_goes18_data()

if __name__ == "__main__":
    logging.info("Script starts")
    main()
    logging.info("Script ends")