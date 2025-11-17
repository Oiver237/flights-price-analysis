import json
import requests
import boto3
import logging
from dotenv import load_dotenv
import os
from serpapi.google_search import GoogleSearch
from datetime import datetime, timedelta


load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_RAW_PREFIX')
API_KEY = os.getenv('API_KEY')


session = boto3.Session(
    aws_access_key_id= AWS_ACCESS_KEY_ID,
    aws_secret_access_key= AWS_SECRET_ACCESS_KEY,
    region_name = AWS_DEFAULT_REGION
)
s3_resource = session.resource('s3')
s3_client = session.client('s3')
bucket = s3_resource.Bucket(S3_BUCKET)



params = {
  "api_key": f"{API_KEY}",
  "engine": "google_flights",
  "hl": "en",
  "gl": "us",
  "departure_id": "CDG",
  "arrival_id": "NSI",
  "outbound_date": "2026-03-15",
  "return_date": "2026-03-30",
  "currency": "EUR"
}

search = GoogleSearch(params)
results = search.get_dict()

output_file = 'output.json'
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=3)

now = datetime.now()
date_str = now.strftime("%d-%m-%Y")
time_str = now.strftime("%H-%M")

s3_key = (f"{S3_PREFIX}/"
    "olivier/"
    f"departure={params.get('departure_id','')}/"
    f"arrival={params.get('arrival_id', '')}/"
    f"date={date_str}/"
    f"time={time_str}/"
    f"{os.path.basename(output_file)}"
    )
print(s3_key)
print(bucket)

bucket.upload_file(Filename = output_file, Key = s3_key)
print('file uploaded successfully!')
# s3.meta.client.upload_file(Filename='input_file_path', Bucket='bucket_name', Key='s3_output_key')
