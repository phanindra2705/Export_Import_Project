import boto3
from __init__ import s3, s3_bucket_name


def upload_to_s3(file_content, file_name):
    s3.put_object(Body=file_content.encode('utf-8'), Bucket=s3_bucket_name, Key=file_name)
    print(f"File '{file_name}' uploaded successfully to S3 bucket '{s3_bucket_name}'")
    return True
