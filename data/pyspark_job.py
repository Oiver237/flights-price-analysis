import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from datetime import datetime
import sys


def main(s3_key):
    print(s3_key)
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

    df = spark.read.option("multiline","true").json(s3_key)

    # Table 1: search_metadata
    search_metadata_df = df.select(
        col("search_metadata.id").alias("search_id"),
        col("search_metadata.status"),
        col("search_metadata.json_endpoint"),
        col("search_metadata.created_at"),
        col("search_metadata.processed_at"),
        col("search_metadata.google_flights_url"),
        col("search_metadata.raw_html_file"),
        col("search_metadata.prettify_html_file"),
        col("search_metadata.total_time_taken")
    ).distinct()

    # Table 2: search_parameters
    search_parameters_df = df.select(
        col("search_metadata.id").alias("search_id"),
        col("search_parameters.engine"),
        col("search_parameters.hl"),
        col("search_parameters.gl"),
        col("search_parameters.departure_id"),
        col("search_parameters.arrival_id"),
        col("search_parameters.outbound_date"),
        col("search_parameters.return_date"),
        col("search_parameters.currency")
    ).distinct()

    # Table 3: price_history
    price_history_df = df.select(
        col("search_metadata.id").alias("search_id"),
        explode("price_insights.price_history").alias("price_history")
    ).select(
        "search_id",
        col("price_history")[0].alias("timestamp"),
        col("price_history")[1].alias("price")
    ).withColumn("date_time", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # Fonction pour traiter les vols (best_flights et other_flights)
    def process_flights_data(flights_column, flight_type):
        # Exploser les vols
        flights_base = df.select(
            col("search_metadata.id").alias("search_id"),
            explode(flights_column).alias("flight_group")
        )
        
        # Extraire les informations des vols avec leg_number
        flights_exploded = flights_base.select(
            col("search_id"),
            col("flight_group.total_duration"),
            col("flight_group.price"),
            col("flight_group.type"),
            col("flight_group.airline_logo"),
            col("flight_group.departure_token"),
            col("flight_group.carbon_emissions.this_flight").alias("carbon_emissions"),
            col("flight_group.carbon_emissions.typical_for_this_route").alias("typical_carbon_emissions"),
            col("flight_group.carbon_emissions.difference_percent").alias("carbon_difference_percent"),
            posexplode("flight_group.flights").alias("leg_number", "flight"),
            lit(flight_type).alias("flight_type")
        )
        
        # Structurer les données des vols
        flights_structured = flights_exploded.select(
            col("search_id"),
            col("departure_token"),
            col("leg_number"),
            col("flight.departure_airport.name").alias("departure_airport_name"),
            col("flight.departure_airport.id").alias("departure_airport_id"),
            col("flight.departure_airport.time").alias("departure_time"),
            col("flight.arrival_airport.name").alias("arrival_airport_name"),
            col("flight.arrival_airport.id").alias("arrival_airport_id"),
            col("flight.arrival_airport.time").alias("arrival_time"),
            col("flight.duration").alias("flight_duration"),
            col("flight.airplane").alias("airplane_type"),
            col("flight.airline").alias("airline"),
            col("flight.airline_logo").alias("airline_logo"),
            col("flight.travel_class").alias("travel_class"),
            col("flight.flight_number").alias("flight_number"),
            col("flight.legroom").alias("legroom"),
            col("flight.extensions").alias("extensions"),
            col("flight.often_delayed_by_over_30_min").alias("often_delayed"),
            col("total_duration").alias("total_itinerary_duration"),
            col("price").alias("itinerary_price"),
            col("carbon_emissions"),
            col("typical_carbon_emissions"),
            col("carbon_difference_percent"),
            col("flight_type"),
            col("type").alias("trip_type")
        )
        
        return flights_structured

    # Traiter best_flights et other_flights
    best_flights_df = process_flights_data("best_flights", "best")
    other_flights_df = process_flights_data("other_flights", "other")

    # Table 4: flights (union de best_flights et other_flights)
    flights_df = best_flights_df.unionByName(other_flights_df)

    # Fonction pour traiter les escales (layovers) - CORRECTION COMPLÈTE
    def process_layovers_data(flights_column, flight_type):
        # Exploser les vols pour obtenir les layovers
        layovers_base = df.select(
            col("search_metadata.id").alias("search_id"),
            explode(flights_column).alias("flight_group")
        )
        
        # Extraire les layovers avec leur numéro
        layovers_exploded = layovers_base.select(
            col("search_id"),
            col("flight_group.departure_token"),
            posexplode("flight_group.layovers").alias("layover_number", "layover"),
            lit(flight_type).alias("flight_type")
        )
        
        # Vérifier si le champ overnight existe dans le schéma
        layover_fields = [field.name for field in layovers_exploded.schema["layover"].dataType]
        
        if "overnight" in layover_fields:
            # Si le champ existe, l'utiliser
            layovers_structured = layovers_exploded.select(
                col("search_id"),
                col("departure_token"),
                col("layover_number"),
                col("layover.duration").alias("layover_duration"),
                col("layover.name").alias("layover_airport_name"),
                col("layover.id").alias("layover_airport_id"),
                col("layover.overnight").alias("is_overnight"),
                col("flight_type")
            )
        else:
            # Si le champ n'existe pas, utiliser False par défaut
            layovers_structured = layovers_exploded.select(
                col("search_id"),
                col("departure_token"),
                col("layover_number"),
                col("layover.duration").alias("layover_duration"),
                col("layover.name").alias("layover_airport_name"),
                col("layover.id").alias("layover_airport_id"),
                lit(False).alias("is_overnight"),  # Valeur par défaut
                col("flight_type")
            )
        
        return layovers_structured

    # Traiter les layovers pour best_flights et other_flights
    best_layovers_df = process_layovers_data("best_flights", "best")
    other_layovers_df = process_layovers_data("other_flights", "other")

    # Table 5: layovers (union de best et other layovers)
    layovers_df = best_layovers_df.unionByName(other_layovers_df)

    flights_final = flights_df \
        .withColumn("departure_datetime", to_timestamp(col("departure_time"))) \
        .withColumn("arrival_datetime", to_timestamp(col("arrival_time"))) \
        .withColumn("price_euros", col("itinerary_price").cast("decimal(10,2)")) \
        .withColumn("search_date", current_date()) \
        .withColumn("year", year(col("departure_datetime"))) \
        .withColumn("month", month(col("departure_datetime"))) \
        .drop("departure_time", "arrival_time")

    # Afficher les résultats
    print("=== SEARCH_METADATA ===")
    search_metadata_df.show(truncate=False)

    print("=== SEARCH_PARAMETERS ===")
    search_parameters_df.show(truncate=False)

    print("=== PRICE_HISTORY ===")
    price_history_df.show(10, truncate=False)

    print("=== FLIGHTS (premiers 10 best) ===")
    best_flights_df.show(10, truncate=False)

    print("=== FLIGHTS (premiers 10 other) ===")
    other_flights_df.show(10, truncate=False)

    print("=== LAYOVERS ===")
    layovers_df.show(10, truncate=False)

    print("=== COMPTES FINAUX ===")
    print(f"Search Metadata: {search_metadata_df.count()}")
    print(f"Search Parameters: {search_parameters_df.count()}")
    print(f"Price History: {price_history_df.count()}")
    print(f"Best Flights: {best_flights_df.count()}")
    print(f"Other Flights: {other_flights_df.count()}")
    print(f"Total Flights: {flights_df.count()}")
    print(f"Best Layovers: {best_layovers_df.count()}")
    print(f"Other Layovers: {other_layovers_df.count()}")
    print(f"Total Layovers: {layovers_df.count()}")

    # Écrire les données dans S3
    now = datetime.now()
    date_str = now.strftime("%d-%m-%Y")
    s3_base_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}/olivier/"

    # Table 1: search_metadata
    search_metadata_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_base_path}/search_metadata/{date_str}/")

    # Table 2: search_parameters
    search_parameters_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_base_path}/search_parameters/{date_str}/")

    # Table 3: price_history
    price_history_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_base_path}/price_history/{date_str}/")

    # Table 4: flights (partitionnée par année et mois)
    flights_final \
        .write.mode("append") \
        .option("compression", "snappy") \
        .partitionBy("year", "month") \
        .parquet(f"{s3_base_path}/flights/{date_str}/")

    # Table 5: layovers
    layovers_df \
        .write.mode("append") \
        .option("compression", "snappy") \
        .parquet(f"{s3_base_path}/layovers/{date_str}/")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        s3_key = sys.argv[1]
        main(s3_key)
    else:
        print("Erreur: Aucun chemin S3 fourni")
        sys.exit(1)