import base64
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, InvalidRegionError
import json
from flask import Flask, json
from flask_mail import Mail
import boto3
import os
from json.decoder import JSONDecodeError


app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Flask-Mail configuration
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'phanindranaidu222@gmail.com'
app.config['MAIL_PASSWORD'] = 'eghe prqy lght udek'
app.config['MAIL_DEFAULT_SENDER'] = ('phanindra', 'phanindranaidu222@gmail.com')

mail = Mail(app)


# AWS S3 configuration
s3 = boto3.client('s3')
s3_bucket_name = 'testingsnowflakebucketwithpython'


# AWS Secrets Manager Client
def get_secret():
    secret_name = "myApp/snowflake"
    region_name = "us-east-2"  # Replace with your actual region

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except NoCredentialsError:
        print("Credentials not available")
        return None
    except PartialCredentialsError:
        print("Incomplete credentials")
        return None
    except InvalidRegionError as e:
        print(f"Invalid region error: {e}")
        return None
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

    secret = get_secret_value_response.get('SecretString') or base64.b64decode(
        get_secret_value_response['SecretBinary'])

    try:
        secret_json = json.loads(secret)
        # print("Retrieved secret:", secret_json)  # Debug print
        return secret_json
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return None


secrets = get_secret()
if secrets is None:
    # print("Failed to retrieve secrets")
    os._exit(1)

snowflake_user = secrets.get('user')
snowflake_password = secrets.get('password')
snowflake_account = secrets.get('account')
snowflake_warehouse = secrets.get('warehouse')
snowflake_database = secrets.get('database')
snowflake_schema = secrets.get('schema')

__all__ = [
    'app',
    's3_bucket_name',
    'snowflake_user',
    'snowflake_password',
    'snowflake_account',
    'snowflake_warehouse',
    'snowflake_database',
    'snowflake_schema'
]


