import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from datetime import datetime

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_CLEANSED_PREFIX')


spark = SparkSession.builder \
        .appName("Json to parquet") \
        .master("spark://spark-master:7077")\
        .config("spark.sql.parquet.compression.codec","snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled","true") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

df = spark.read.option("multiline","true").json("/opt/spark/shared-data/output.json")
# df.show(10)
# df.printSchema()

# search_id_df = df.select(col("search_metadata.id").alias("search_id"))

flight_schema = StructType([
    StructField("departure_airport", StructType([
        StructField("name", StringType()),
        StructField("id", StringType()),
        StructField("time", StringType()),
    ])),
    StructField("arrival_airport", StructType([
        StructField("name", StringType()),
        StructField("id", StringType()),
        StructField("time", StringType()),
    ])),
    StructField("duration", IntegerType()),
    StructField("airplane", StringType()),
    StructField("airline", StringType()),
    StructField("airline_logo", StringType()),
    StructField("travel_class", StringType()),
    StructField("flight_number", StringType()),
    StructField("legroom", StringType()),
#     StructField("ticket_also_sold_by", ArrayType()),
    StructField("often_delayed_by_over_30_min", BooleanType())
])


layover_schema = StructType([
    StructField("duration", IntegerType()),
    StructField("name", StringType()),
    StructField("id", StringType()),
#     StructField("overnight", StringType()),
])


carbon_emissions_schema = StructType([
    StructField("this_flight", IntegerType()),
    StructField("typical_for_this_route", IntegerType()),
    StructField("difference_percent", IntegerType())
])


search_metadata_df = df.select(
    col("search_metadata.id").alias("search_id"),
    col("search_metadata.created_at").alias("search_timestamp"),
    col("search_metadata.total_time_taken").alias("processing_time_seconds"),
    col("search_metadata.google_flights_url").alias("google_flights_url")
)

search_paramaters_df= df.select(
    col("search_metadata.id").alias("search_id"),
    col("search_parameters.*")
)

best_flights_base = df.select(
    col("search_metadata.id").alias("search_id"),
    explode("best_flights").alias("flight_group")
)

best_flights_base.show(10)


flights_exploded = best_flights_base.select(
    col("search_id"),
    col("flight_group.total_duration"),
    col("flight_group.price"),
    col("flight_group.type"),
    col("flight_group.airline_logo"),
    col("flight_group.departure_token"),
    col("flight_group.carbon_emissions").alias("co2_emissions"),
    posexplode("flight_group.flights").alias("flight_leg", "flight")
)
flights_exploded.show(10)


layovers_exploded = best_flights_base.select(
    col("search_id"),
    posexplode("flight_group.layovers").alias("layover_leg","layover")
)

best_flights_combined = flights_exploded.alias("f").join(
    layovers_exploded.alias("l"),
    (col("f.search_id") == col("l.search_id")) & 
    (col("f.flight_leg") == col("l.layover_leg")),
    "left"
)

# best_flights_exploded = df \
#         .select(explode("best_flights").alias("flight_group")) \
#         .select(
#             col("flight_group.total_duration"),
#             col("flight_group.price"),
#             col("flight_group.type"),
#             col("flight_group.airline_logo"),
#             col("flight_group.departure_token"),
#             col("flight_group.carbon_emissions").alias("co2_emissions"),
#             explode("flight_group.flights").alias("flight_leg", "flight"),
#             explode("flight_group.layovers").alias("layover_leg", "layover"),
#         )

flights_structured = best_flights_combined.select(
    col("f.search_id").alias("search_id"),  # Explicitly use f.search_id
    col("f.flight_leg").alias("leg_number"),
    col("f.flight.departure_airport.name").alias("departure_airport_name"),
    col("f.flight.departure_airport.id").alias("departure_airport_id"),
    col("f.flight.departure_airport.time").alias("departure_time"),
    col("f.flight.arrival_airport.name").alias("arrival_airport_name"),
    col("f.flight.arrival_airport.id").alias("arrival_airport_id"),
    col("f.flight.arrival_airport.time").alias("arrival_time"),
    col("f.flight.duration").alias("flight_duration_minutes"),
    col("f.flight.airplane").alias("airplane_type"),
    col("f.flight.airline").alias("airline"),
    col("f.flight.travel_class").alias("travel_class"),
    col("f.flight.flight_number").alias("flight_number"),
    col("f.total_duration").alias("itinerary_total_duration"),
    col("f.price").alias("itinerary_price"),
    col("f.co2_emissions.this_flight").alias("co2_emissions_grams"),
    col("f.co2_emissions.difference_percent").alias("co2_vs_typical_percent"),
    col("l.layover.duration").alias("layover_duration_minutes"),
    col("l.layover.name").alias("layover_airport_name"),
    col("l.layover.id").alias("layover_airport_id"),
    col("f.departure_token")
)

flights_final = flights_structured \
        .withColumn("departure_datetime", to_timestamp(col("departure_time"))) \
        .withColumn("arrival_datetime", to_timestamp(col("arrival_time"))) \
        .withColumn("price_euros", col("itinerary_price").cast("decimal(10,2)") ) \
        .withColumn("departure_datetime", to_timestamp(col("departure_time"))) \
        .withColumn("search_date", current_date()) \
        .withColumn("year", year(col("departure_datetime"))) \
        .withColumn("month", month(col("departure_datetime"))) \
        .drop("departure_time", "arrival_time")

price_insights_df_timestamp = df.select(
    col("search_metadata.id").alias("search_id"),
    col("price_insights.lowest_price").alias("lowest_price_euros"),
    col("price_insights.price_level").alias("price_level"),
    col("price_insights.typical_price_range")[0].alias("typical_price_min"),
    col("price_insights.typical_price_range")[1].alias("typical_price_max"),
    explode("price_insights.price_history").alias("price_history")
).select(
    "search_id",
    "lowest_price_euros",
    "price_level",
    "typical_price_min",
    "typical_price_max",
    col("price_history")[0].alias("timestamp"),
    col("price_history")[1].alias("historical_price")
)

price_insights_df = price_insights_df_timestamp \
                .withColumn("date_time", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                .drop("timestamp")

print("=== FLIGHTS FINAL ===")
flights_final.show(10, truncate=False)
print("=== PRICE INSIGHTS ===")
price_insights_df.show(10)
print("=== SEARCH METADATA ===")
search_metadata_df.show(10)
print("=== SEARCH PARAMETERS ===")
search_paramaters_df.show(10)

now = datetime.now()
date_str = now.strftime("%d-%m-%Y")
s3_key = (f"s3a://{S3_BUCKET}/{S3_PREFIX}/"
    "olivier/"
    )

flights_final \
        .write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("year", "month") \
        .parquet(f"{s3_key}/flights_parquet/{date_str}/")

price_insights_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_key}/price_history/{date_str}/")

search_paramaters_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_key}/search_paramater/{date_str}/")

search_metadata_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_key}/search_metadata/{date_str}/")

spark.stop()