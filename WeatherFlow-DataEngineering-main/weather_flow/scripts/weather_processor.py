#!/usr/bin/env python3
"""
Spark processor for weather data.
This script processes raw weather data files and creates processed output.
"""

import os
import sys
import json
import argparse
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, explode, lit, to_timestamp, window
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
except ImportError:
    logger.error("PySpark is not installed. This script requires PySpark to run.")
    sys.exit(1)

def create_spark_session():
    """Create and return a Spark session."""
    return (
        SparkSession.builder
        .appName("WeatherDataProcessor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

def define_schema():
    """Define the schema for the weather data."""
    return StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("source", StringType(), True)
    ])

def process_weather_data(spark, input_path, output_path):
    """
    Process weather data using Spark.
    
    Args:
        spark: SparkSession
        input_path: Path to raw weather data
        output_path: Path to save processed data
    """
    logger.info(f"Processing weather data from {input_path} to {output_path}")
    
    # Check if input directory exists and has files
    if not os.path.exists(input_path):
        logger.error(f"Input path {input_path} does not exist")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Define schema
    schema = define_schema()
    
    try:
        # Read all JSON files from the input directory
        df = spark.read.schema(schema).json(f"{input_path}/*.json")
        
        if df.count() == 0:
            logger.warning("No data found to process")
            return
        
        logger.info(f"Loaded {df.count()} records")
        
        # Basic transformations
        processed_df = df.withColumn(
            "temperature_fahrenheit", 
            (col("temperature") * 9/5) + 32
        ).withColumn(
            "processed_timestamp", 
            lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        )
        
        # Calculate heat index for high temperatures
        processed_df = processed_df.withColumn(
            "heat_index_fahrenheit",
            (col("temperature_fahrenheit") > 80).cast("int") * (
                -42.379 + 
                2.04901523 * col("temperature_fahrenheit") + 
                10.14333127 * col("humidity") - 
                0.22475541 * col("temperature_fahrenheit") * col("humidity") - 
                0.00683783 * col("temperature_fahrenheit") * col("temperature_fahrenheit") - 
                0.05481717 * col("humidity") * col("humidity") + 
                0.00122874 * col("temperature_fahrenheit") * col("temperature_fahrenheit") * col("humidity") + 
                0.00085282 * col("temperature_fahrenheit") * col("humidity") * col("humidity") - 
                0.00000199 * col("temperature_fahrenheit") * col("temperature_fahrenheit") * col("humidity") * col("humidity")
            )
        )
        
        # Convert heat index back to Celsius
        processed_df = processed_df.withColumn(
            "heat_index_celsius",
            (col("heat_index_fahrenheit") - 32) * 5/9
        )
        
        # Determine weather severity
        processed_df = processed_df.withColumn(
            "weather_severity",
            (
                ((col("temperature") > 35) | (col("temperature") < -10)).cast("int") * lit("high") +
                (((col("temperature") > 30) & (col("temperature") <= 35)) | 
                 ((col("temperature") < 0) & (col("temperature") >= -10))).cast("int") * lit("moderate") +
                ((col("temperature") <= 30) & (col("temperature") >= 0)).cast("int") * lit("normal")
            )
        )
        
        # Save processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{output_path}/weather_processed_{timestamp}"
        
        # Save as JSON
        processed_df.coalesce(1).write.mode("overwrite").json(f"{output_file}.json")
        
        # Save as Parquet (more efficient for analytics)
        processed_df.coalesce(1).write.mode("overwrite").parquet(f"{output_file}.parquet")
        
        logger.info(f"Successfully processed and saved data to {output_file}")
        
        # Show some statistics
        logger.info("Weather data statistics:")
        processed_df.describe(["temperature", "humidity", "pressure"]).show()
        
        # Count by city
        logger.info("Records by city:")
        processed_df.groupBy("city").count().show()
        
    except Exception as e:
        logger.error(f"Error processing weather data: {e}")
        raise

def main():
    """Main function to run the Spark processor."""
    parser = argparse.ArgumentParser(description='Process weather data with Spark')
    parser.add_argument('--input', required=True, help='Input directory with raw weather data')
    parser.add_argument('--output', required=True, help='Output directory for processed data')
    
    # If running in Spark, get arguments from sys.argv
    if len(sys.argv) > 1:
        args = parser.parse_args()
        input_path = args.input
        output_path = args.output
    else:
        # Default paths for testing
        input_path = "/opt/airflow/data/raw"
        output_path = "/opt/airflow/data/processed"
    
    logger.info(f"Starting weather data processing from {input_path} to {output_path}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process data
        process_weather_data(spark, input_path, output_path)
        
        logger.info("Weather data processing completed successfully")
    except Exception as e:
        logger.error(f"Failed to process weather data: {e}")
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
