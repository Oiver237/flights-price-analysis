from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from serpapi.google_search import GoogleSearch
import logging
import boto3
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'projet-fil-rouge',
    'depends_on_past' : False,
    'start_date': datetime(2025,1,1),
    'retries': 2,
    'retries_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False,

}


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
    spark_submit_task = SparkSubmitOperator(
        task_id='process_json_spark',
        application='/opt/airflow/dags/scripts/pyspark_job.py',
        name='flight_data_processing',
        conn_id='spark_default',
        verbose=True,
        conf= {
            "spark.master": "spark://spark-master:7077",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.eventLog.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4",
        application_args=[
            "{{ ti.xcom_pull(task_ids='upload_raw_data_to_s3', key='total_s3_key') }}"
        ]
    )
    # spark_submit_task = BashOperator(
    # task_id='process_json_spark',
    # bash_command='docker exec spark-master /opt/spark/bin/spark-submit '
    #              '--master spark://spark-master:7077 '
    #              '--packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4 '
    #              '--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider '
    #              '/opt/spark/shared-data/pyspark_job.py '
    #              '{{ ti.xcom_pull(task_ids="upload_raw_data_to_s3", key="total_s3_key") }}'
    #             )
    clean_up_task = PythonOperator(
        task_id='clean_up_folder',
        python_callable=clean_up_folder,
        provide_context=True
    )

download_task >> upload_raw_data_task >> [spark_submit_task, clean_up_task]



