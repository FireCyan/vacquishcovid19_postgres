'''
AWS functions such as 
- S3 upload/download 
- RDS connection

Author: john.yang 2021-04-17
'''
import git
repo = git.Repo('.', search_parent_directories=True)
repo_loc = repo.working_tree_dir

import io
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import configparser
import os
import psycopg2
from pathlib import Path
# from pandas.core.frame import DataFrame

import boto3
from botocore.exceptions import NoCredentialsError

# local_comp = False

host = "covid19.cluster-cuwrjlvxl5qu.ap-southeast-2.rds.amazonaws.com"
# Before 20240615 update, was "covid19.cuwrjlvxl5qu.ap-southeast-2.rds.amazonaws.com"
username = "postgres"
port="5432"

# if any(local_comp_user in os.environ.get("USERNAME") for local_comp_user in ['john', 'cyan8']):
if os.environ.get("USERNAME") is not None:
    cred = configparser.ConfigParser()
    cred.read(repo_loc + '/config/credential.cfg')
    ACCESS_KEY = cred['AWS_USER']['ACCESS_KEY']
    SECRET_KEY = cred['AWS_USER']['SECRET_KEY']

    password = cred['RDS']['PASSWORD']
    port = "5432"


else:
    
    username="cyan8388"
    REGION="ap-southeast-2"

    #gets the credentials from .aws/credentials
    session = boto3.Session(profile_name=username)
    client = session.client('rds')

    password = client.generate_db_auth_token(DBHostname=host, Port=port, DBUsername=username, Region=REGION) 


###################################
# AWS RDS connection
###################################

def conn_default():
    # conn = psycopg2.connect("host=127.0.0.1 dbname=covid19_db user=postgres password=localtest")
    conn = psycopg2.connect(
        host=host,
        user=username,
        password=password,
        port=port)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return conn, cur


def conn_db(dbname, autoc=True):
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        user=username,
        password=password,
        port=port)
    conn.set_session(autocommit=autoc)
    cur = conn.cursor()

    return conn, cur



###################################
# Upload to S3 and list S3 files
###################################

def upload_to_aws(df, fname, bucket='covid19-case-vac-data'):
    # https://medium.com/bilesanmiahmad/how-to-upload-a-file-to-amazon-s3-in-python-68757a1867c6
    # if using local
    if os.environ.get("USERNAME") == 'john':
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    else:
        s3 = boto3.client('s3')

    csv_buffer = io.StringIO()
    try:
        # s3.upload_file(local_file, bucket, s3_file)
        # https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
        df.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, fname).put(Body=csv_buffer.getvalue())
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def list_files(folder, bucket='covid19-case-vac-data', ext='.csv'):
    # https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3

    if os.environ.get("USERNAME") == 'john':
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    else:
        s3 = boto3.client('s3')

    s3_resource = boto3.resource('s3')
    # conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
    my_bucket = s3_resource.Bucket(bucket)
    # return my_bucket

    list_files = []
    for o in my_bucket.objects.filter(Prefix=folder, Delimiter='/'):
        # print(o.key)
        if (ext in o.key):
            list_files.append(o.key.split('/')[-1])
            # print(o.key.split('/')[-1])

    return list_files
    # test = s3.list_objects(Bucket=bucket_name)
    # print(test)
    # for key in s3.list_objects(Bucket=bucket_name)['Contents']:
    #     print(key['Key'])


def _read_aws_csv(fname, bucket='covid19-case-vac-data'):
    if os.environ.get("USERNAME") == 'john':
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    else:
        s3 = boto3.client('s3')

    csv_obj = s3.get_object(Bucket=bucket, Key=fname)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(io.StringIO(csv_string), index_col=0)

    return df



pd.read_aws_csv = _read_aws_csv