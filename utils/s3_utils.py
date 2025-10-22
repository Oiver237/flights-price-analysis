# utils/s3_utils.py
import boto3
from botocore.exceptions import NoCredentialsError

def upload_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client('s3')
    if not object_name:
        object_name = file_name
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"{file_name} uploadé vers s3://{bucket}/{object_name}")
    except NoCredentialsError:
        print("Problème d'identifiants AWS")
    except Exception as e:
        print(f"Erreur upload S3: {e}")
