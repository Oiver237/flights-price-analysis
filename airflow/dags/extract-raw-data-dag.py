import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from serpapi.google_search import GoogleSearch
import logging
import boto3
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
        'API_KEY'
    ]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    logger.info('No missing env variables')

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

validate_environment_variables()

default_args = {
    'owner': 'projet-fil-rouge',
    'depends_on_past' : False,
    'start_date': datetime(2025,1,1),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False,

}

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', '')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', '')
aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
s3_bucket = os.getenv('S3_BUCKET', '')
s3_cleansed_prefix = os.getenv('S3_CLEANSED_PREFIX', '')

def download_flight_data(**kwargs):
    API_KEY = os.getenv('API_KEY')
    params = {
        "api_key": f'{API_KEY}',
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
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=3)

    kwargs['ti'].xcom_push(key='output_file_path', value=output_file)
    logger.info(f'File downloaded successfully to {output_file}')


def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    output_file_path = ti.xcom_pull(task_ids='download_flight_data', key='output_file_path')
    
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
    S3_BUCKET = os.getenv('S3_BUCKET')
    S3_PREFIX = os.getenv('S3_RAW_PREFIX')


    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )

    s3_ressource = session.resource('s3')
    bucket = s3_ressource.Bucket(S3_BUCKET)

    now = datetime.now()
    date_str = now.strftime('%d-%m-%Y')
    time_str = now.strftime('%H-%M')

    s3_key = (f'{S3_PREFIX}/'
              "olivier/"
              f"departure=CDG/"
              f"arrival=NSI/"
              f"date={date_str}/"
              f"time={time_str}/"
              f"flight_data.json"
              )

    bucket.upload_file(Filename=output_file_path, Key=s3_key)
    #path for spark job
    total_s3_key = f's3a://{S3_BUCKET}/{s3_key}'
    kwargs['ti'].xcom_push(key='total_s3_key', value=total_s3_key)
    logger.info(f'File uploaded successfully to s3://{S3_BUCKET}/{s3_key}')

def run_spark_job(**kwargs):
    ti = kwargs['ti']
    s3_key = ti.xcom_pull(task_ids='upload_raw_data_to_s3', key='total_s3_key')

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    spark_command = [
        '/opt/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4',
        '--conf', f'spark.hadoop.fs.s3a.access.key={aws_access_key}',
        '--conf', f'spark.hadoop.fs.s3a.secret.key={aws_secret_key}',
        '--conf', 'spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        '--conf', 'spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com',
        '/opt/spark/shared-data/pyspark_job.py',
        s3_key
    ]
    try:
        result = subprocess.run(
            spark_command,
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode == 0:
            logger.info('Spark job completed successfully')
            logger.info(f'Spark output: {result.stdout} ')
        else:
            logger.error(f'Spark job failed with return code: {result.returncode}')
            logger.error(f'Spark stderr: {result.stderr}')
    except subprocess.TimeoutExpired:
        logger.error('Spark job timed out after 10 min')
        raise
    except Exception as e:
        logger.error(f'Failed to run spark job due to: {str(e)}')
        raise


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
    description= 'Process flight data',
    schedule_interval= '@daily',
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
    spark_submit_task = PythonOperator(
    task_id='process_json_spark',
    python_callable=run_spark_job,
    )
    clean_up_task = PythonOperator(
        task_id='clean_up_folder',
        python_callable=clean_up_folder,
        provide_context=True
    )

download_task >> upload_raw_data_task >> spark_submit_task>> clean_up_task



