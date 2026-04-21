"""
Spark processor for weather data analysis.
"""

import os
import sys
import json
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, max, min, count, expr, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, MapType

# Define paths directly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')

# Create directories if they don't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

# Configuration constants
SPARK_MASTER = 'local[*]'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_RAW = 'weather-raw'
KAFKA_TOPIC_PROCESSED = 'weather-processed'

def create_spark_session(app_name="WeatherDataProcessor"):
    """
    Create and return a Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Use the Homebrew installed Spark
    import os
    
    # Set environment variables to use Homebrew Spark installation
    os.environ['SPARK_HOME'] = '/usr/local/Cellar/apache-spark/3.5.5/libexec'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    
    # Create a basic Spark session for batch processing
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
            .getOrCreate())

def define_weather_schema():
    """
    Define the schema for weather data.
    
    Returns:
        StructType: Spark schema for weather data
    """
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("clouds", DoubleType(), True),
        StructField("coordinates", 
                   StructType([
                       StructField("lat", DoubleType(), True),
                       StructField("lon", DoubleType(), True)
                   ]), True),
        StructField("raw_data", StringType(), True)
    ])

def process_batch_data(spark, input_path=None, output_path=None):
    """
    Process weather data in batch mode.
    
    Args:
        spark: SparkSession
        input_path: Path to input data files
        output_path: Path to write output data
    """
    # Use the data directory if no input path is provided
    if input_path is None:
        input_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
                                 "data", "raw")
    
    # Use the processed data directory if no output path is provided
    if output_path is None:
        output_path = PROCESSED_DATA_PATH
    
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Read all JSON files from the input path
    df = spark.read.option("multiline", "true").json(f"{input_path}/*.json")
    
    # Show the dataframe schema
    print("Original Data Schema:")
    df.printSchema()
    
    # Show a sample of the data
    print("Sample Data:")
    df.show(5, truncate=False)
    
    # Process the data
    processed_df = process_weather_data(df)
    
    # Write the processed data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processed_df.write.mode("overwrite").parquet(f"{output_path}/weather_processed_{timestamp}.parquet")
    
    # Also save as JSON for easier human reading
    processed_df.write.mode("overwrite").json(f"{output_path}/weather_processed_{timestamp}.json")
    
    print(f"Processed data written to {output_path}")

def process_streaming_data(spark):
    """
    Process weather data in streaming mode from Kafka.
    
    Args:
        spark: SparkSession
    """
    # Define the schema for the weather data
    weather_schema = define_weather_schema()
    
    # Read from Kafka
    kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC_RAW)
                .option("startingOffsets", "latest")
                .load())
    
    # Parse the value from Kafka
    parsed_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    # Convert JSON string to structured data
    weather_df = parsed_df.select(
        from_json(col("value"), weather_schema).alias("data")
    ).select("data.*")
    
    # Process the data
    processed_df = process_weather_data(weather_df)
    
    # Convert back to JSON for writing to Kafka
    kafka_output = processed_df.select(
        to_json(struct("*")).alias("value")
    )
    
    # Write to Kafka
    kafka_query = (kafka_output
                  .writeStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                  .option("topic", KAFKA_TOPIC_PROCESSED)
                  .option("checkpointLocation", "/tmp/kafka-checkpoint")
                  .start())
    
    # Write to file system as well
    file_query = (processed_df
                 .writeStream
                 .format("parquet")
                 .option("path", PROCESSED_DATA_PATH)
                 .option("checkpointLocation", "/tmp/file-checkpoint")
                 .trigger(processingTime="1 minute")
                 .start())
    
    # Wait for the queries to terminate
    kafka_query.awaitTermination()
    file_query.awaitTermination()

def process_weather_data(df):
    """
    Process weather data with Spark transformations.
    
    Args:
        df: Spark DataFrame with weather data
        
    Returns:
        DataFrame: Processed weather data
    """
    # Add processing timestamp
    processed_df = df.withColumn("processed_timestamp", 
                               lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")))
    
    # Convert temperature to Fahrenheit if it's in Celsius
    processed_df = processed_df.withColumn(
        "temperature_fahrenheit", 
        expr("temperature * 9/5 + 32")
    )
    
    # Calculate heat index for conditions where it's applicable
    processed_df = processed_df.withColumn(
        "heat_index_fahrenheit",
        expr("""
            CASE 
                WHEN temperature * 9/5 + 32 > 80 THEN
                    -42.379 + 2.04901523 * (temperature * 9/5 + 32) 
                    + 10.14333127 * humidity
                    - 0.22475541 * (temperature * 9/5 + 32) * humidity 
                    - 0.00683783 * pow((temperature * 9/5 + 32), 2)
                    - 0.05481717 * pow(humidity, 2) 
                    + 0.00122874 * pow((temperature * 9/5 + 32), 2) * humidity
                    + 0.00085282 * (temperature * 9/5 + 32) * pow(humidity, 2) 
                    - 0.00000199 * pow((temperature * 9/5 + 32), 2) * pow(humidity, 2)
                ELSE NULL
            END
        """)
    )
    
    # Convert heat index back to Celsius
    processed_df = processed_df.withColumn(
        "heat_index_celsius",
        expr("(heat_index_fahrenheit - 32) * 5/9")
    )
    
    # Add weather severity level based on conditions
    processed_df = processed_df.withColumn(
        "weather_severity",
        expr("""
            CASE 
                WHEN lower(weather_condition) LIKE '%thunderstorm%' OR 
                     lower(weather_condition) LIKE '%tornado%' OR 
                     lower(weather_condition) LIKE '%hurricane%' OR 
                     lower(weather_condition) LIKE '%blizzard%' THEN 'severe'
                WHEN lower(weather_condition) LIKE '%rain%' OR 
                     lower(weather_condition) LIKE '%snow%' OR 
                     lower(weather_condition) LIKE '%drizzle%' OR 
                     lower(weather_condition) LIKE '%fog%' OR 
                     lower(weather_condition) LIKE '%mist%' OR 
                     lower(weather_condition) LIKE '%shower%' THEN 'moderate'
                WHEN lower(weather_condition) LIKE '%cloudy%' OR 
                     lower(weather_condition) LIKE '%overcast%' OR 
                     lower(weather_condition) LIKE '%partly cloudy%' THEN 'mild'
                ELSE 'normal'
            END
        """)
    )
    
    return processed_df

def main():
    """Main function to process weather data."""
    # Create Spark session
    spark = create_spark_session()
    
    # Process data in batch mode
    process_batch_data(spark)
    
    # Process data in streaming mode (uncomment to enable)
    # process_streaming_data(spark)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
