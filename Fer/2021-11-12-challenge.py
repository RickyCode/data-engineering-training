import csv
import json

import boto3

# # Leer un archivo en un bucket de S3
key = "datalake/ingestion-layer/raw-zone/fer/2021-11-18-challange/pokemonesKanto.csv"
bucket = "data-engineering-training"


def lambda_handler(event, context):
    # # leyendo el archivo csv
    s3_resource = boto3.resource("s3")
    s3_object = s3_resource.Object(bucket, key)
    data = s3_object.get()["Body"].read().decode("utf-8").splitlines()

    lines = csv.reader(data)
    headers = next(lines)
    print("headers: %s" % (headers))
    for line in lines:
        print(line[0], line[1], line[2], line[3])

    s3_client = boto3.client("s3")
    csvfile = s3_client.get_object(Bucket=bucket, Key=key)
    csvcontent = csvfile["Body"].read().decode("utf-8")

    print("csv", csvcontent)
