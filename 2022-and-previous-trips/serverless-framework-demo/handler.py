import json


def add_group_name():
    return "data engineering training"


def hello(event, context):
    body = {
        "message": f"Go Serverless v3.0! Your function executed successfully! {add_group_name()}",
        "event": event,
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
