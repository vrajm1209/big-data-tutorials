import os
import boto3
import time
import pandas as pd

def scrape_goes18_data():

    """Function scrapes the publically available amazon s3 bucket for GOES-18 satellite radar data,
    all sub folders for the pre-defined product are selected and appended into a dictionary, the final return 
    value is a dataframe. Initially, the function sets up 2 boto3 clients (to connect to AWS): 1 for accessing s3 buckets
    and the other for accessing AWS CloudWatch to perform logging to a log group and log stream. Both these clients have their
    own AWS access & secret key generated from AWS with necessary permissions.
    -----
    Returns:
    A dataframe containing path for all subfolders 
    """

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

    #intialise dictionary to store scraped data before moving it to a sqllite table
    scraped_goes18_dict = {
        'id': [],
        'product': [],
        'year': [],
        'day': [],
        'hour': []
    }

    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Scraping data from GOES18 bucket"
            }
        ]
    )

    id=1    #for storing as primary key in db
    prefix = "ABI-L1b-RadC/"    #just one product to consider as per scope of assignment
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
                scraped_goes18_dict['id'].append(id)   #map all scraped data into the dict
                scraped_goes18_dict['product'].append(sub_sub_path[0])
                scraped_goes18_dict['year'].append(sub_sub_path[1])
                scraped_goes18_dict['day'].append(sub_sub_path[2])
                scraped_goes18_dict['hour'].append(sub_sub_path[3])
                id+=1

    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "db-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Data scraped successfully"
            }
        ]
    )
      
    scraped_goes18_df = pd.DataFrame(scraped_goes18_dict)     #final scraped metadata stored in dataframe
    return scraped_goes18_df    #the dataframe containing all scraped data