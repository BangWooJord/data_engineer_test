import re
import json
import pyodbc
import boto3
import botocore
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime


bucket_name = 'data-engineering-interns.macpaw.io'
object_key = 'files_list.data'

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))

try: #downloading initial file
    s3.Bucket(bucket_name).download_file(object_key, 'files_list.data')
except botocore.exceptions.ClientError as e:
    if e:
        print("Caught an error: " + e.response['Error']['Code'])
        exit(1)
    else:
        raise

connection = pyodbc.connect(r'Driver={SQL Server};'
                            r'Server=.\SQLEXPRESS;'
                            r'Database=macpaw;'
                            r'Trusted_Connection=yes;')
cursor = connection.cursor()

# reading initial file
list_file = open('files_list.data', 'r')
files = list_file.readlines()

for file_name in files:
    file_name = file_name.strip()
    # downloading  json files
    s3.Bucket(bucket_name).download_file(file_name, 'json/' + file_name)
    data_file = open('json/' + file_name)
    data = json.load(data_file)
    for data_obj in data:
        if data_obj['type'] == 'song' or data_obj['type'] == 'app' or data_obj['type'] == 'movie':
            query = 'INSERT INTO '
            query += data_obj['type'] + 's('
            # putting keys into a query like "INSERT INTO songs(key, key, key)
            for key in data_obj['data'].keys():
                query += key + ','

            # adding additional columns, that are not listed in a json file
            if data_obj['type'] == 'song':
                query += 'ingestion_time'
            elif data_obj['type'] == 'app':
                query += 'is_awesome'
            else:
                query += 'original_title_normalized'

            query += ') VALUES('

            # putting values into a query
            for key in data_obj['data'].keys():
                query += 'N\'' + str(data_obj['data'][key]).replace("\'", "\'\'") + '\'' + ','
            if data_obj['type'] == 'song':
                query += '\'' + datetime.now().strftime("%Y/%m/%d %H:%M:%S") + '\''
            elif data_obj['type'] == 'app':
                query += '\'' + str(data_obj['data']['rating'] > 4) + '\''
            else:
                normalized_title = data_obj['data']['original_title']
                normalized_title = re.sub('[^a-zA-Z ]', '', normalized_title)
                normalized_title = normalized_title.replace(' ', '_')
                normalized_title = normalized_title.lower()
                query += '\'' + normalized_title + '\''

            query += ')'
            # executing query
            cursor.execute(query)

connection.commit()


