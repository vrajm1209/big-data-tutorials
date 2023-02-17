import os
import boto3
import time
from dotenv import load_dotenv

#load env variables and change logging level to info
load_dotenv()

def list_files_in_goes18_bucket(user_product, user_year, user_day, user_hour):

    """Function traverses through the specified GOES-18 S3 folder based on the inputs given. The file names are appended to a list. 
    Initially, the function sets up 2 boto3 clients (to connect to AWS): 1 for accessing s3 buckets
    and the other for accessing AWS CloudWatch to perform logging to a log group and log stream. Both these clients have their
    own AWS access & secret key generated from AWS with necessary permissions.
    -----
    Input parameters:
    user_product : str
        string containing the product name within the GOES18 S3 bucket
    user_year : str
        string containing the year to find files in the GOES18 S3 bucket
    user_day : str
        string containing the day to find files in the GOES18 S3 bucket
    user_hour : str
        string containing the hour to find files in the GOES18 S3 bucket
    -----
    Returns:
    A list containing all file names 
    """

    #authenticate S3 client with your user credentials that are stored in your .env config file
    s3resource = boto3.resource('s3',
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

    files = []  #list to store all file names within folder
    prefix = user_product+'/'+user_year+'/'+user_day+'/'+user_hour+'/'
    goes18_bucket = s3resource.Bucket(os.environ.get('GOES18_BUCKET_NAME'))
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "s3-bucket-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Printing files from GOES18 S3 bucket"
            }
        ]
    )
    for objects in goes18_bucket.objects.filter(Prefix=prefix):
        file_path = objects.key
        file_path = file_path.split('/')
        files.append(file_path[-1])

    return files

def list_files_in_nexrad_bucket(user_year, user_month, user_day, user_ground_station):

    """Function traverses through the specified NEXRAD S3 folder based on the inputs given. The file names are appended to a list. 
    Initially, the function sets up 2 boto3 clients (to connect to AWS): 1 for accessing s3 buckets
    and the other for accessing AWS CloudWatch to perform logging to a log group and log stream. Both these clients have their
    own AWS access & secret key generated from AWS with necessary permissions.
    -----
    Input parameters:
    user_year : str
        string containing the year within the NEXRAD S3 bucket
    user_month : str
        string containing the month to find files in the NEXRAD S3 bucket
    user_day : str
        string containing the day to find files in the NEXRAD S3 bucket
    user_ground_station : str
        string containing the ground station to find files in the NEXRAD S3 bucket
    -----
    Returns:
    A list containing all file names 
    """

    #authenticate S3 client with your user credentials that are stored in your .env config file
    s3resource = boto3.resource('s3',
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

    files = []
    prefix = user_year+'/'+user_month+'/'+user_day+'/'+user_ground_station+'/'
    nexrad_bucket = s3resource.Bucket(os.environ.get('NEXRAD_BUCKET_NAME'))
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "s3-bucket-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Printing files from NEXRAD S3 bucket"
            }
        ]
    )
    for objects in nexrad_bucket.objects.filter(Prefix=prefix):
        file_path = objects.key
        file_path = file_path.split('/')
        files.append(file_path[-1])

    return files

def copy_goes_file_to_user_bucket(selected_file_name, user_product, user_year, user_day, user_hour):

    """Function copies the selected file (from GOES-18 bucket) to the user bucket. It takes in the file name and product,
    year, day, hour selections to find the file and make a copy of it on the user bucket. If the file already exists in
    the user bucket then it does not re-copy to file from the GOES location. Initially, the function sets up 2 boto3 clients (to connect to AWS): 1 for accessing s3 buckets
    and the other for accessing AWS CloudWatch to perform logging to a log group and log stream. Both these clients have their
    own AWS access & secret key generated from AWS with necessary permissions.
    -----
    Input parameters:
    selected_file_name : str
        string containing the file name (with extension, if any)
    user_product : str
        string containing the product name within the GOES18 S3 bucket
    user_year : str
        string containing the year to find files in the GOES18 S3 bucket
    user_day : str
        string containing the day to find files in the GOES18 S3 bucket
    user_hour : str
        string containing the hour to find files in the GOES18 S3 bucket
    -----
    Returns:
    A URL (str) to download/access the copied file from the user bucket location 
    """

    #authenticate S3 client with your user credentials that are stored in your .env config file
    s3resource = boto3.resource('s3',
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

    destination_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))  #define the destination bucket as the user bucket
    all_selections_string = user_product+'/'+user_year+'/'+user_day+'/'+user_hour+'/'+selected_file_name
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "user-input-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Download requested for GOES file selections: " + all_selections_string + " & file name: " + selected_file_name
            }
        ]
    )
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "s3-bucket-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Attempting to copy selected GOES file " + selected_file_name + " to local S3 bucket"
            }
        ]
    )

    destination_folder = 'goes18/'
    destination_key = destination_folder + selected_file_name
    url_to_mys3 = 'https://sevir-bucket-01.s3.amazonaws.com/' + destination_key
    copy_source = {     #define the copy source bucket as GOES18 bucket
        'Bucket': os.environ.get('GOES18_BUCKET_NAME'),
        'Key': all_selections_string
        }
    
    for file in destination_bucket.objects.all():
        if(file.key == destination_key):    #if selected file already exists at destination bucket
            clientLogs.put_log_events(      #logging to AWS CloudWatch logs
                logGroupName = "assignment01-logs",
                logStreamName = "s3-bucket-logs",
                logEvents = [
                    {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "File exists at destination bucket"
                    }
                ]
            )
            clientLogs.put_log_events(      #logging to AWS CloudWatch logs
                logGroupName = "assignment01-logs",
                logStreamName = "user-input-logs",
                logEvents = [
                    {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "Displaying download link for already existing file "+ selected_file_name + " with selections " + all_selections_string
                    }
                ]
            )
            return url_to_mys3

    destination_bucket.copy(copy_source, destination_key)   #copy file to destination bucket
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "s3-bucket-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "File copied to S3 bucket successfully"
            }
        ]
    )
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "user-input-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Displaying download link for copied file "+ selected_file_name + " with selections " + all_selections_string
            }
        ]
    )

    return url_to_mys3

def copy_nexrad_file_to_user_bucket(selected_file_name, user_year, user_month, user_day, user_ground_station):

    """Function copies the selected file (from NEXRAD bucket) to the user bucket. It takes in the file name and year,
    month, day, ground station selections to find the file and make a copy of it on the user bucket. If the file already 
    exists in the user bucket then it does not re-copy to file from the NEXRAD location. Initially, the function sets up 2 
    boto3 clients (to connect to AWS): 1 for accessing s3 buckets and the other for accessing AWS CloudWatch to perform 
    logging to a log group and log stream. Both these clients have their own AWS access & secret key generated from AWS 
    with necessary permissions.
    -----
    Input parameters:
    selected_file_name : str
        string containing the file name (with extension, if any)
    user_year : str
        string containing the year within the NEXRAD S3 bucket
    user_month : str
        string containing the month to find files in the NEXRAD S3 bucket
    user_day : str
        string containing the day to find files in the NEXRAD S3 bucket
    user_ground_station : str
        string containing the ground station to find files in the NEXRAD S3 bucket
    -----
    Returns:
    A URL (str) to download/access the copied file from the user bucket location 
    """

    #authenticate S3 client with your user credentials that are stored in your .env config file
    s3resource = boto3.resource('s3',
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

    destination_bucket = s3resource.Bucket(os.environ.get('USER_BUCKET_NAME'))  #define the destination bucket as the user bucket
    all_selections_string = user_year+'/'+user_month+'/'+user_day+'/'+user_ground_station+'/'+selected_file_name
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "user-input-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Download requested for NEXRAD file selections: " + all_selections_string + " & file name: " + selected_file_name
            }
        ]
    )
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "s3-bucket-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Attempting to copy selected NEXRAD file " + selected_file_name + " to local S3 bucket"
            }
        ]
    )
    destination_folder = 'nexrad/'
    destination_key = destination_folder + selected_file_name
    url_to_mys3 = 'https://sevir-bucket-01.s3.amazonaws.com/' + destination_key
    copy_source = {     #define the copy source bucket as NEXRAD bucket
        'Bucket': os.environ.get('NEXRAD_BUCKET_NAME'),
        'Key': all_selections_string
        }
    
    for file in destination_bucket.objects.all():
        if(file.key == destination_key):    #if selected file already exists at destination bucket
            clientLogs.put_log_events(      #logging to AWS CloudWatch logs
                logGroupName = "assignment01-logs",
                logStreamName = "s3-bucket-logs",
                logEvents = [
                    {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "File exists at destination bucket"
                    }
                ]
            )
            clientLogs.put_log_events(      #logging to AWS CloudWatch logs
                logGroupName = "assignment01-logs",
                logStreamName = "user-input-logs",
                logEvents = [
                    {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "Displaying download link for already existing file "+ selected_file_name + " with selections " + all_selections_string
                    }
                ]
            )
            return url_to_mys3

    destination_bucket.copy(copy_source, destination_key)   #copy file to destination bucket
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "s3-bucket-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "File copied to S3 bucket successfully"
            }
        ]
    )
    clientLogs.put_log_events(      #logging to AWS CloudWatch logs
        logGroupName = "assignment01-logs",
        logStreamName = "user-input-logs",
        logEvents = [
            {
            'timestamp' : int(time.time() * 1e3),
            'message' : "Displaying download link for copied file "+ selected_file_name + " with selections " + all_selections_string
            }
        ]
    )

    return url_to_mys3