import json


def add_group_name():
    return "data engineering training"


def bye(event, context):
    body = {
        "message": f"Bye, good weekend {add_group_name()} team! Good job!",
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
