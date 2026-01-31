import boto3
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
REGION = os.getenv('AWS_DEFAULT_REGION', 'eu-north-1')

print(f"Testing S3 Connection...")
print(f"Bucket: {BUCKET_NAME}")
print(f"Region: {REGION}")
print(f"Key ID: {ACCESS_KEY[:5]}... (Redacted)")

try:
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )
    # List files
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=5)
    if 'Contents' in response:
        print("Success! Found files:")
        for obj in response['Contents']:
            print(f" - {obj['Key']}")
    else:
        print("Success! Bucket is empty or no files found.")
except Exception as e:
    print(f"FAILED: {e}")
