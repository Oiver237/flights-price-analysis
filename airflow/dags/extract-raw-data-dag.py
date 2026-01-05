
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import json
import os
from serpapi.google_search import GoogleSearch
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_environment_variables():
    """Validate that all required environment variables are set"""
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_DEFAULT_REGION',
        'S3_BUCKET',
        'S3_RAW_PREFIX',
        'S3_CLEANSED_PREFIX',
        'SERPAPI_KEYS',
        'CITY_IDS'
    ]
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    logger.info('No missing env variables' if not missing_vars else f"Missing: {missing_vars}")
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")


validate_environment_variables()

default_args = {
    'owner': 'projet-fil-rouge',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False,
}

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', '')
aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
s3_bucket = os.getenv('S3_BUCKET', 'projet-fil-rouge-s3-dev')
s3_cleansed_prefix = os.getenv('S3_CLEANSED_PREFIX', 'cleansed-data')


def get_next_api_key():
    serpapi_keys_str = os.getenv('SERPAPI_KEYS', '')
    api_keys = [k.strip() for k in serpapi_keys_str.split(',') if k.strip()]
    logger.info(f'We have {len(api_keys)} API keys')
    if not api_keys:
        raise ValueError('No serpapi keys found in variable env')
    current_index = int(Variable.get('serpapi_current_key_index', default_var=0))
    api_key_to_use = api_keys[current_index]
    next_index = (current_index + 1) % len(api_keys)
    Variable.set('serpapi_current_key_index', next_index)
    logger.info(f"Using API key index {current_index}, next run will use index {next_index}")
    return api_key_to_use


def get_next_city():
    city_id_str = os.getenv('CITY_IDS', '')
    city_ids = [c.strip() for c in city_id_str.split(',') if c.strip()]
    logger.info(f'We have a list of {len(city_ids)} cities.')
    if not city_ids:
        raise ValueError('No city ids found in the environment.')
    current_index = int(Variable.get('city_current_id_index', default_var=0))
    city_id_to_use = city_ids[current_index]
    next_index = (current_index + 1) % len(city_ids)
    Variable.set('city_current_id_index', next_index)
    logger.info(f'Using {city_ids[current_index]} as arrival city.')
    return city_id_to_use


def download_flight_data(**kwargs):
    API_KEY = get_next_api_key()
    city = get_next_city()
    logger.info(f'Using API key: ******{API_KEY[-5:]}')
    params = {
        "api_key": f'{API_KEY}',
        "engine": "google_flights",
        "hl": "fr",
        "gl": "fr",
        "departure_id": "CDG",
        "arrival_id": f"{city}",
        "outbound_date": "2026-03-15",
        "return_date": "2026-03-30",
        "currency": "EUR",
        "deep_search": True
    }
    search = GoogleSearch(params)
    results = search.get_dict()
    if len(results.keys()) <= 1:
        raise ValueError(f"Expected more than 1 key in flight data, got {len(results.keys())} keys. Results: {results}")
    output_file = 'output.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=3)
    kwargs['ti'].xcom_push(key='output_file_path', value=output_file)
    kwargs['ti'].xcom_push(key='arrival_city', value=city)
    logger.info(f'File downloaded successfully to {output_file}')


def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    output_file_path = ti.xcom_pull(task_ids='download_flight_data', key='output_file_path')
    arrival_city = ti.xcom_pull(task_ids='download_flight_data', key='arrival_city')

    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    s3_ressource = session.resource('s3')
    bucket = s3_ressource.Bucket(os.getenv('S3_BUCKET'))

    now = datetime.now()
    date_str = now.strftime('%d-%m-%Y')
    time_str = now.strftime('%H-%M')
    s3_key = (
        f"{os.getenv('S3_RAW_PREFIX')}/"
        f"departure=CDG/"
        f"arrival={arrival_city}/"
        f"date={date_str}/"
        f"time={time_str}/"
        f"flight_data.json"
    )
    bucket.upload_file(Filename=output_file_path, Key=s3_key)

    # path for spark job
    total_s3_key = f"s3a://{os.getenv('S3_BUCKET')}/{s3_key}"
    kwargs['ti'].xcom_push(key='total_s3_key', value=total_s3_key)
    logger.info(f'File uploaded successfully to s3://{os.getenv("S3_BUCKET")}/{s3_key}')


# def run_spark_job(**kwargs):
#     ti = kwargs['ti']
#     s3_key = ti.xcom_pull(task_ids='upload_raw_data_to_s3', key='total_s3_key')
#     logger.info(f'Job spark running for: {s3_key}')

#     spark_command = [
#         '/opt/spark/bin/spark-submit',
#         '--verbose',
#         '--master', 'spark://spark-master:7077',
#         # S3A configs (SimpleAWSCredentialsProvider for static keys)
#         '--conf', f'spark.hadoop.fs.s3a.access.key={os.getenv("AWS_ACCESS_KEY_ID")}',
#         '--conf', f'spark.hadoop.fs.s3a.secret.key={os.getenv("AWS_SECRET_ACCESS_KEY")}',
#         '--conf', 'spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
#         '--conf', 'spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com',
#         '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
#         '/opt/spark/shared-data/pyspark_job.py',
#         s3_key
#     ]

#     try:
#         result = subprocess.run(
#             spark_command,
#             capture_output=True,
#             text=True,
#             timeout=600
#         )
#         logger.info(f"Spark stdout:\n{result.stdout}")
#         logger.error(f"Spark stderr:\n{result.stderr}")  # keep stderr visible in logs
#         if result.returncode != 0:
#             raise AirflowException(f"Spark job failed with return code {result.returncode}")
#         logger.info('Spark job completed successfully')
#     except subprocess.TimeoutExpired:
#         logger.error('Spark job timed out after 10 min')
#         raise
#     except Exception as e:
#         logger.error(f'Failed to run spark job due to: {str(e)}')
#         raise


def clean_up_folder(**kwargs):
    ti = kwargs['ti']
    output_file_path = ti.xcom_pull(task_ids='download_flight_data', key='output_file_path')
    try:
        os.remove(output_file_path)
        logger.info(f'Local file {output_file_path} cleaned up')
    except OSError as e:
        logger.warning(f'Could not remove local file {output_file_path}: {e}')


with DAG(
    'flight_data_pipeline',
    default_args=default_args,
    description='Process flight data',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['flights', 's3']
) as dag:
    download_task = PythonOperator(
        task_id='download_flight_data',
        python_callable=download_flight_data,
        provide_context=True
    )

    upload_raw_data_task = PythonOperator(
        task_id='upload_raw_data_to_s3',
        python_callable=upload_to_s3,
        provide_context=True
    )


    clean_up_task = PythonOperator(
        task_id='clean_up_folder',
        python_callable=clean_up_folder,
        provide_context=True
    )

download_task >> upload_raw_data_task >> clean_up_task
