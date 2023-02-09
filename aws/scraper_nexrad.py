import os
import boto3
import time
import pandas as pd
from dotenv import load_dotenv

#load env variables and change logging level to info
load_dotenv()

#authenticate S3 client with your user credentials that are stored in your .env config file
s3client = boto3.client('s3',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

#authenticate S3 client for logging with your user credentials that are stored in your .env config file
clientLogs = boto3.client('logs',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_LOG_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_LOG_SECRET_KEY')
                        )

#intialise dictionary to store scrapped data before moving it to a sqllite table
scrapped_nexrad_dict = {
    'id': [],
    'year': [],
    'month': [],
    'day': [],
    'ground_station': []
}

def scrape_nexrad_data():
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Scrapping data from NEXRAD bucket"
            }
        ]
    )

    id=1    #for storing as primary key in db
    years_to_scrape = ['2022', '2023']      #considering only 2 years as per scope of assignment

    for year in years_to_scrape:
        prefix = year+"/"    #replace this with user input from streamlit UI with / in end
        result = s3client.list_objects(Bucket=os.environ.get('NEXRAD_BUCKET_NAME'), Prefix=prefix, Delimiter='/')
        #travesing into each subfolder and store the folder names within each
        for o in result.get('CommonPrefixes'):
            path = o.get('Prefix').split('/')
            prefix_2 = prefix + path[-2] + "/"      #new prefix with added subdirectory path
            sub_folder = s3client.list_objects(Bucket=os.environ.get('NEXRAD_BUCKET_NAME'), Prefix=prefix_2, Delimiter='/')
            for p in sub_folder.get('CommonPrefixes'):
                sub_path = p.get('Prefix').split('/')
                prefix_3 = prefix_2 + sub_path[-2] + "/"    #new prefix with added subdirectory path
                sub_sub_folder = s3client.list_objects(Bucket=os.environ.get('NEXRAD_BUCKET_NAME'), Prefix=prefix_3, Delimiter='/')
                for q in sub_sub_folder.get('CommonPrefixes'):
                    sub_sub_path = q.get('Prefix').split('/')   #remove the filename from the path
                    scrapped_nexrad_dict['id'].append(id)   #map all scrapped data into the dict
                    scrapped_nexrad_dict['year'].append(sub_sub_path[0])
                    scrapped_nexrad_dict['month'].append(sub_sub_path[1])
                    scrapped_nexrad_dict['day'].append(sub_sub_path[2])
                    scrapped_nexrad_dict['ground_station'].append(sub_sub_path[3])
                    id+=1

    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Data scrapped successfully"
            }
        ]
    )
      
    scrapped_nexrad_df = pd.DataFrame(scrapped_nexrad_dict)     #final scrapped metadata stored in dataframe
    return scrapped_nexrad_df

def main():
    metadata_nexrad = scrape_nexrad_data()

if __name__ == "__main__":
    main()