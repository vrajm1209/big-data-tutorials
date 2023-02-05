import os
import boto3
import logging
from dotenv import load_dotenv
import pandas as pd

#load env variables and change logging level to info
load_dotenv()
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs.log')

#authenticate S3 client with your user credentials that are stored in your .env config file
s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

#intialise dictionary to store scrapped data before moving it to a sqllite table
scrapped_goes18_dict = {
    'id': [],
    'product': [],
    'year': [],
    'day': [],
    'hour': []
}

def scrape_goes18_data():
    id=1    #for storing as primary key in db
    logging.info("Scrapping data from GOES18 bucket")
    prefix = "ABI-L1b-RadC/"    #replace this with user input from streamlit UI with / in end
    result = s3client.list_objects(Bucket=os.environ.get('GOES18_BUCKET_NAME'), Prefix=prefix, Delimiter='/')
    #traversing into each subfolder and store the folder names within each
    for o in result.get('CommonPrefixes'):
        path = o.get('Prefix').split('/')
        prefix_2 = prefix + path[-2] + "/"      #new prefix with added subdirectory path
        sub_folder = s3client.list_objects(Bucket=os.environ.get('GOES18_BUCKET_NAME'), Prefix=prefix_2, Delimiter='/')
        for p in sub_folder.get('CommonPrefixes'):
            sub_path = p.get('Prefix').split('/')
            prefix_3 = prefix_2 + sub_path[-2] + "/"    #new prefix with added subdirectory path
            sub_sub_folder = s3client.list_objects(Bucket=os.environ.get('GOES18_BUCKET_NAME'), Prefix=prefix_3, Delimiter='/')
            for q in sub_sub_folder.get('CommonPrefixes'):
                sub_sub_path = q.get('Prefix').split('/')
                sub_sub_path = sub_sub_path[:-1]    #remove the filename from the path
                scrapped_goes18_dict['id'].append(id)   #map all scrapped data into the dict
                scrapped_goes18_dict['product'].append(sub_sub_path[0])
                scrapped_goes18_dict['year'].append(sub_sub_path[1])
                scrapped_goes18_dict['day'].append(sub_sub_path[2])
                scrapped_goes18_dict['hour'].append(sub_sub_path[3])
                id+=1

    logging.info("Data scrapped successfully")       
    scrapped_goes18_df = pd.DataFrame(scrapped_goes18_dict) #final scrapped metadata stored in dataframe
    return scrapped_goes18_df

def main():
    metadata_goes18 = scrape_goes18_data()

if __name__ == "__main__":
    logging.info("GOES18 scraper script starts")
    main()
    logging.info("GOES18 scraper script ends")