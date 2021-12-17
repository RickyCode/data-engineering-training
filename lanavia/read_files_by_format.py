import json

import awswrangler as wr

def lambda_handler(event, context):
    name_bucket = 'data-engineering-training'
    name_key_csv = 'datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.csv'
    
    bucketUri = f's3://{name_bucket}/{name_key_csv}'
    print(bucketUri)
    raw_df_csv=wr.s3.read_csv(path=bucketUri)
    print(raw_df_csv)
    
    name_key_json = 'datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.json'
    bucketUri = f's3://{name_bucket}/{name_key_json}'
    print(bucketUri)
    raw_df_json = wr.s3.read_json(path=bucketUri,  lines=True)
    print(raw_df_json)
    
    name_key_parquet = 'datalake/ingestion-layer/raw-zone/2021-11-26-challenge/internet_usage_by_countries.parquet'
    bucketUri = f's3://{name_bucket}/{name_key_parquet}'
    print(bucketUri)
    raw_df_parquet = wr.s3.read_parquet(path=bucketUri)
    print(raw_df_parquet)


'''import json
import boto3

import awswrangler as wr

def lambda_handler(event, context):
    s3 = boto3.client("s3", region_name='us-east-2')
    files=[]
    name_bucket = 'data-engineering-training'
    name_key = 'datalake/ingestion-layer/raw-zone/2021-11-26-challenge/'
    for obj in s3.list_objects(Bucket=name_bucket, Prefix=name_key)['Contents']:
        #print(" Type of the object " + str(type(obj)))
        
        file = "s3://"+str(obj['Key'])
        print(file)
        #match a particular pattern .
        if file.endswith('.csv'):
            raw_df=wr.s3.read_csv(path=file)
            #for df in raw_df:
            #    print(df) 
            
            
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }'''
