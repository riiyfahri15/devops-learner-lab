import json
import os

import boto3


secret_name = os.environ.get("SECRET_NAME", "devops/rds-credentials")
region_name = os.environ.get("AWS_REGION", "us-west-2")

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

get_secret_value_response = client.get_secret_value(
    SecretId=secret_name
)
print(get_secret_value_response)

secret = get_secret_value_response['SecretString']

secret = json.loads(secret)   

conn = psycopg2.connect(
        host=secret["host"],
        port=secret["port"],
        dbname=secret["db_name"],
        user=secret["db_user"],
        password=secret["db_password"],
        connect_timeout=10,
    )