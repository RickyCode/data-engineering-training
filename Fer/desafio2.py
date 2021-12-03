import json
import boto3
import csv
import pandas as pd

keycsv ='datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.csv'
keyjson='datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.json'
keyparquet='datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.parquet'
bucket ='data-engineering-training'
def lambda_handler(event,context):
    ##leyendo el archivo csv
    s3_resource = boto3.resource('s3')
         
    s3_client=boto3.client('s3')
    csvfile = s3_client.get_object(Bucket = bucket, Key = keycsv)
    csvcontent = csvfile['Body'].read().decode('utf-8')
 
    print("csv",csvcontent)
    
    ##Leyendo el json
    s3json= boto3.resource('s3')
    content_object=s3json.Object(bucket,keyjson)
    ##file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.load(content_objec.get()['Body'])
    print(json_content)