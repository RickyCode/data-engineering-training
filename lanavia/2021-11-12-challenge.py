import json
import boto3

s3 = boto3.client("s3", region_name='us-east-2')

def lambda_handler(event, context):
    print("Executing lambda_handler")
    files=[]
    name_bucket = 'data-engineering-training'
    name_key = 'datalake/ingestion-layer/raw-zone/lanavia/2021-11-12-challenge'
    for obj in s3.list_objects(Bucket=name_bucket, Prefix=name_key)['Contents']:
        #print(" Type of the object " + str(type(obj)))
        print(str(obj['Key']))
        file_from_directory = s3.get_object(Bucket=name_bucket, Key=obj['Key'])
        content_of_file =file_from_directory['Body'].read().decode('utf-8') 
        print(content_of_file)
        files.append(content_of_file)

    return {
        'statusCode': 200,
        'body': files
    }

