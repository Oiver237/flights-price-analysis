from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from datetime import datetime
import sys
import logging


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def main(s3_key):
    logger = setup_logging()
    logger.info(f"Starting processing for S3 key: {s3_key}")
    
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
    S3_BUCKET = os.getenv('S3_BUCKET')
    S3_PREFIX = os.getenv('S3_CLEANSED_PREFIX')

    # Validate required environment variables
    required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET', 'S3_CLEANSED_PREFIX']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return 3

    spark = None
    try:
        logger.info("Creating Spark session")
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

        logger.info("Spark session created successfully")

        try:
            logger.info(f"Reading JSON data from {s3_key}")
            df = spark.read.option("multiline","true").json(s3_key)
            logger.info(f"Successfully read JSON data. DataFrame schema: {df.schema}")
            logger.info(f"Record count: {df.count()}")
        except Exception as e:
            logger.error(f"Failed to read JSON data from {s3_key}", exc_info=True)
            raise

        try:
            arrival_city_row = df.select("search_parameters.arrival_id").first()
            arrival_city = arrival_city_row[0] if arrival_city_row else "unknown"
            logger.info(f"Extracted arrival city: {arrival_city}")
        except Exception as e:
            logger.error("Failed to extract arrival city", exc_info=True)
            arrival_city = "unknown"

        # Process search_metadata
        try:
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
            logger.info("Successfully processed search_metadata")
        except Exception as e:
            logger.error("Failed to process search_metadata", exc_info=True)
            raise

        # Process search_parameters
        try:
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
            logger.info("Successfully processed search_parameters")
        except Exception as e:
            logger.error("Failed to process search_parameters", exc_info=True)
            raise

        # Process price_history
        try:
            price_history_df = df.select(
                col("search_metadata.id").alias("search_id"),
                explode("price_insights.price_history").alias("price_history")
            ).select(
                "search_id",
                col("price_history")[0].alias("timestamp"),
                col("price_history")[1].alias("price")
            ).withColumn("date_time", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            logger.info("Successfully processed price_history")
        except Exception as e:
            logger.error("Failed to process price_history", exc_info=True)
            raise

        def process_flights_data(flights_column, flight_type):
            try:
                logger.info(f"Processing {flight_type} flights data")
                flights_base = df.select(
                    col("search_metadata.id").alias("search_id"),
                    explode(flights_column).alias("flight_group")
                )
                
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
                
                logger.info(f"Successfully processed {flight_type} flights data")
                return flights_structured
            except Exception as e:
                logger.error(f"Failed to process {flight_type} flights data", exc_info=True)
                raise

        # Process best_flights and other_flights
        try:
            best_flights_df = process_flights_data("best_flights", "best")
            other_flights_df = process_flights_data("other_flights", "other")
            flights_df = best_flights_df.unionByName(other_flights_df)
            logger.info("Successfully processed all flights data")
        except Exception as e:
            logger.error("Failed to process flights data", exc_info=True)
            raise

        def process_layovers_data(flights_column, flight_type):
            try:
                logger.info(f"Processing {flight_type} layovers data")
                layovers_base = df.select(
                    col("search_metadata.id").alias("search_id"),
                    explode(flights_column).alias("flight_group")
                )

                layovers_exploded = layovers_base.select(
                    col("search_id"),
                    col("flight_group.departure_token"),
                    posexplode("flight_group.layovers").alias("layover_number", "layover"),
                    lit(flight_type).alias("flight_type")
                )
                
                layover_fields = [field.name for field in layovers_exploded.schema["layover"].dataType]
                
                if "overnight" in layover_fields:
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
                
                logger.info(f"Successfully processed {flight_type} layovers data")
                return layovers_structured
            except Exception as e:
                logger.error(f"Failed to process {flight_type} layovers data", exc_info=True)
                raise

        # Process layovers
        try:
            best_layovers_df = process_layovers_data("best_flights", "best")
            other_layovers_df = process_layovers_data("other_flights", "other")
            layovers_df = best_layovers_df.unionByName(other_layovers_df)
            logger.info("Successfully processed all layovers data")
        except Exception as e:
            logger.error("Failed to process layovers data", exc_info=True)
            raise

        # Final processing for flights
        try:
            logger.info("Performing final flights processing")
            flights_final = flights_df \
                .withColumn("departure_datetime", to_timestamp(col("departure_time"))) \
                .withColumn("arrival_datetime", to_timestamp(col("arrival_time"))) \
                .withColumn("price_euros", col("itinerary_price").cast("decimal(10,2)")) \
                .withColumn("search_timestamp", current_timestamp()) \
                .withColumn("year", year(col("departure_datetime"))) \
                .withColumn("month", month(col("departure_datetime"))) \
                .drop("departure_time", "arrival_time")
            logger.info("Successfully completed final flights processing")
        except Exception as e:
            logger.error("Failed in final flights processing", exc_info=True)
            raise

        # Display results
        try:
            logger.info("=== SEARCH_METADATA ===")
            search_metadata_df.show(truncate=False)

            logger.info("=== SEARCH_PARAMETERS ===")
            search_parameters_df.show(truncate=False)

            logger.info("=== PRICE_HISTORY ===")
            price_history_df.show(10, truncate=False)

            logger.info("=== FLIGHTS (premiers 10 best) ===")
            best_flights_df.show(10, truncate=False)

            logger.info("=== FLIGHTS (premiers 10 other) ===")
            other_flights_df.show(10, truncate=False)

            logger.info("=== LAYOVERS ===")
            layovers_df.show(10, truncate=False)

            logger.info("=== COMPTES FINAUX ===")
            logger.info(f"Search Metadata: {search_metadata_df.count()}")
            logger.info(f"Search Parameters: {search_parameters_df.count()}")
            logger.info(f"Price History: {price_history_df.count()}")
            logger.info(f"Best Flights: {best_flights_df.count()}")
            logger.info(f"Other Flights: {other_flights_df.count()}")
            logger.info(f"Total Flights: {flights_df.count()}")
            logger.info(f"Best Layovers: {best_layovers_df.count()}")
            logger.info(f"Other Layovers: {other_layovers_df.count()}")
            logger.info(f"Total Layovers: {layovers_df.count()}")
        except Exception as e:
            logger.warning("Error during display operations, but continuing with write operations", exc_info=True)

        # Write data to S3
        now = datetime.now()
        date_str = now.strftime("%d-%m-%Y")
        time_str = now.strftime("%H-%M")
        s3_base_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}/olivier/"

        write_operations = [
            ("search_metadata", search_metadata_df),
            ("search_parameters", search_parameters_df),
            ("price_history", price_history_df),
            ("flights", flights_final),
            ("layovers", layovers_df)
        ]

        successful_writes = 0
        total_writes = len(write_operations)

        for table_name, dataframe in write_operations:
            try:
                s3_path = f"{s3_base_path}/{table_name}/arrival_city={arrival_city}/date={date_str}/time={time_str}"
                logger.info(f"Writing {table_name} to S3: {s3_path}")
                dataframe \
                    .write.mode("append") \
                    .option("compression", "snappy") \
                    .parquet(s3_path)
                logger.info(f"Successfully wrote {table_name} to S3")
                successful_writes += 1
            except Exception as e:
                logger.error(f"Failed to write {table_name} to S3", exc_info=True)

        logger.info(f"Write operations completed: {successful_writes}/{total_writes} successful")

        if successful_writes == total_writes:
            logger.info("SUCCESS: All operations completed successfully!")
            return 0
        else:
            logger.warning(f"PARTIAL SUCCESS: Only {successful_writes}/{total_writes} operations completed successfully")
            return 1

    except Exception as e:
        logger.critical("Job failed with exception", exc_info=True)
        return 2
    finally:
        if spark is not None:
            try:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error("Error while stopping Spark session", exc_info=True)


if __name__ == "__main__":
    logger = setup_logging()
    if len(sys.argv) > 1:
        s3_key = sys.argv[1]
        exit_code = main(s3_key)
        logger.info(f"Job completed with exit code: {exit_code}")
        sys.exit(exit_code)
    else:
        logger.error("No S3 path provided")
        sys.exit(1)