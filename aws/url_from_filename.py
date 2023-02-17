import os
import boto3
import re
import requests
import time
from dotenv import load_dotenv

#load env variables and change logging level to info
load_dotenv()

#authenticate S3 client for logging with your user credentials that are stored in your .env config file
clientLogs = boto3.client('logs',
                        region_name='us-east-1',
                        aws_access_key_id = os.environ.get('AWS_LOG_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_LOG_SECRET_KEY')
                        )

goes_file_name = "OR_ABI-L1b-RadC-M6C01_G18_s20222090001140_e20222090003513_c20222090003553.nc"
goes_file_name2 = "OR_ABI-L2-DMWVM-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc"
goes_file_name3 = "OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c202228009345744.nc"
nexrad_file_name = "KLWX19931112_005128"
nexrad_file_name2 = "FOP120230101_005524_V06_MDM"
nexrad_file_name3 = "KBOX20030717_014733.gz"
 
def generate_goes_url(file_name):
    input_url = "https://noaa-goes18.s3.amazonaws.com/"
    file_name = file_name.strip()   #strip for any whitespaces

    #match the input filename with the filename format required for GOES18 satellite (CASE SENSITIVE)
    if (re.match(r'[O][R][_][A-Z]{3}[-][A-Za-z0-9]{2,3}[-][A-Za-z0-9]{4,6}[-][A-Z0-9]{2,5}[_][G][1][8][_][s][0-9]{14}[_][e][0-9]{14}[_][c][0-9]{14}\b', file_name)):
        file_list = file_name.split("_")
        sublist=file_list[1].split("-") #splitting components of filename entered in order to create URL
        if (sublist[2].isalpha()) is False:
            sublist[2] = sublist[2][:-1]
        sublist_date = file_list[3]

        #forming URL using split up components from entered filename
        final_url = input_url+"-".join(sublist[0:3])+"/"+sublist_date[1:5]+"/"+sublist_date[5:8]+"/"+sublist_date[8:10]+"/"+file_name
        response = requests.get(final_url)
        if(response.status_code == 404):    #if format is correct but no such file exists
            clientLogs.put_log_events(      #logging to AWS CloudWatch logs
                logGroupName = "assignment01-logs",
                logStreamName = "user-input-logs",
                logEvents = [
                    {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "No such file exists at GOES18 location"
                    }
                ]
            )
            return -1

        #else provide URL
        clientLogs.put_log_events(      #logging to AWS CloudWatch logs
            logGroupName = "assignment01-logs",
            logStreamName = "user-input-logs",
            logEvents = [
                {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "Successfully found URL for given file name for GOES18; \nFilename requested for download: " + file_name
                }
            ]
        )
        return final_url

    else:   #in case the filename format provided by user is wrong
        clientLogs.put_log_events(      #logging to AWS CloudWatch logs
            logGroupName = "assignment01-logs",
            logStreamName = "user-input-logs",
            logEvents = [
                {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "Invalid filename format for GOES18"
                }
            ]
        )
        return 1
        
def generate_nexrad_url(file_name):
    input_url = "https://noaa-nexrad-level2.s3.amazonaws.com/"
    file_name = file_name.strip()   #strip for any whitespaces

    #match the input filename with the filename format required for NEXRAD Level 2 satellite files (CASE SENSITIVE)
    if (re.match(r'[A-Z]{3}[A-Z0-9][0-9]{8}[_][0-9]{6}[_]{0,1}[A-Z]{0,1}[0-9]{0,2}[_]{0,1}[A-Z]{0,3}\b', file_name)):
        #forming URL using filename provided
        final_url = input_url+file_name[4:8]+"/"+file_name[8:10]+"/"+file_name[10:12]+"/"+file_name[:4]+"/"+file_name
        response = requests.get(final_url)
        if(response.status_code == 404):    #if format is correct but no such file exists
            clientLogs.put_log_events(      #logging to AWS CloudWatch logs
                logGroupName = "assignment01-logs",
                logStreamName = "user-input-logs",
                logEvents = [
                    {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "No such file exists at NEXRAD location"
                    }
                ]
            )
            return -1
        
        #else provide URL
        clientLogs.put_log_events(      #logging to AWS CloudWatch logs
            logGroupName = "assignment01-logs",
            logStreamName = "user-input-logs",
            logEvents = [
                {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "Successfully found URL for given file name for NEXRAD \nFilename requested for download: " + file_name
                }
            ]
        )
        return final_url

    else:   #in case the filename format provided by user is wrong
        clientLogs.put_log_events(      #logging to AWS CloudWatch logs
            logGroupName = "assignment01-logs",
            logStreamName = "user-input-logs",
            logEvents = [
                {
                    'timestamp' : int(time.time() * 1e3),
                    'message' : "Invalid filename format for NEXRAD"
                }
            ]
        )
        return 1