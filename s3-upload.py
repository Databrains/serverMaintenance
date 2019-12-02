import boto3
import botocore
from datetime import datetime, timedelta
import os
import platform

def findS3Object(s3, s3Bucket, key):
    try:
        object = s3.head_object(Bucket=s3Bucket, Key=key)
        print(object)
        status = True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            status = False
    return status


filePrefix = "tsbackup-"
fileSuffix = ".tsbak"

secret = 'ZpV/WuBNFSw5UKbyoB+I7EjowwZyVmANXUAZHgOU'
accessKey = 'AKIAQP5HUBNXWV2TSIWS'



runDate = datetime.now()
previousDate = runDate - timedelta(days=7)
fileDate = runDate.strftime('%Y-%m-%d')
previousFileDate = previousDate.strftime('%Y-%m-%d')
oldFile = filePrefix + fileDate + fileSuffix
backupFilePath = '/var/opt/tableau/tableau_server/data/tabsvc/files/backups/'
serverName = platform.uname()
serverName = serverName[1]
key = 'ip-172-31-10-183tsbackup-2019-11-26'
s3Bucket = "dbi-tableau-backups"
previousFileName = filePrefix + previousFileDate + fileSuffix
fileName = filePrefix + fileDate + fileSuffix
curBkupFilePath = backupFilePath + fileName
prevBkupFilePath = backupFilePath + fileName
s3fileName = serverName + fileName
print(fileName)
"""Moves new file to s3"""
if os.path.exists(curBkupFilePath):
    print('Local Backup File  Exists')
    #, aws_access_key_id=accessKey, aws_secret_access_key=secret, region_name='us-east-1'
    # s3 = boto3.client('s3')
    s3 = boto3.client('s3', aws_access_key_id=accessKey, aws_secret_access_key=secret, region_name='us-east-1')
    s3.upload_file(curBkupFilePath, s3Bucket, s3fileName)
    s3.list_objects_v2()
    print('File Uploaded')
    if findS3Object(s3, s3Bucket, previousFileName):
        print('Previous Backup File Exists in S3')
        previousFile = s3.object(s3Bucket, previousFileName)
        previousFile.delete()
        if os.path.exists(prevBkupFilePath):
            os.remove(prevBkupFilePath)
    else:
        print('Previous Backup File Does Not Exist in S3')



else:
    print('Local Backup File Does Not Exist')










# print(serverName)
# print(backupFilePath + oldFile)
# print(curBkupFilePath)