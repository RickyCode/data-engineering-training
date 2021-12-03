import json
import boto3



keycsv ='datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.csv'
keyjson='datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.json'
keyparquet='datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.parquet'
bucket ='data-engineering-training'
def lambda_handler(event,context):
    ##leyendo el archivo csv
  
    s3_client=boto3.client('s3')
    csvfile = s3_client.get_object(Bucket = bucket, Key = keycsv)
    csvcontent = csvfile['Body'].read().decode('utf-8')
    print("csv",csvcontent)
    
    ##Leyendo el json
    s3= boto3.resource('s3')
    content_object=s3.Object(bucket,keyjson)
    file_content = content_object.get()['Body'].read().decode('utf-8').split()

    print(file_content)
    
  
   