import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
        .appName("Json to parquet") \
        .master("spark://spark-master:7077")\
        .config("spark.sql.parquet.compression.codec","snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()


df = spark.read.option("multiline","true").json("/opt/bitmani/spark/json-to-parquet.py")
df.show(10)
df.printSchema()