#!/usr/bin/env python3
"""
Spark processor for weather data in the WeatherFlow project.
This script processes weather data using Spark for the DS463 Data Engineering final project.
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
    from pyspark.sql.functions import col, from_json, explode, lit, to_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
except ImportError:
    logger.error("PySpark is not installed. This script requires PySpark to run.")
    sys.exit(1)

def create_spark_session():
    """Create and return a Spark session."""
    return (
        SparkSession.builder
        .appName("WeatherFlow-DataProcessor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .master("local[*]")  # Use local mode for simplicity
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
        StructField("timestamp", StringType(), True),
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
        # If no files exist yet, create a sample dataset for demonstration
        json_files = [f for f in os.listdir(input_path) if f.endswith('.json')]
        
        if not json_files:
            logger.warning("No JSON files found in input directory. Creating sample data for demonstration.")
            # Create sample data for demonstration
            sample_data = [
                {
                    "city": "New York", 
                    "country": "USA", 
                    "latitude": 40.7128, 
                    "longitude": -74.0060,
                    "temperature": 22.5, 
                    "humidity": 65, 
                    "pressure": 1013.2,
                    "wind_speed": 5.2, 
                    "wind_direction": 270,
                    "description": "Partly cloudy", 
                    "timestamp": datetime.now().isoformat(),
                    "source": "Sample"
                },
                {
                    "city": "London", 
                    "country": "UK", 
                    "latitude": 51.5074, 
                    "longitude": -0.1278,
                    "temperature": 18.3, 
                    "humidity": 72, 
                    "pressure": 1011.5,
                    "wind_speed": 4.1, 
                    "wind_direction": 225,
                    "description": "Light rain", 
                    "timestamp": datetime.now().isoformat(),
                    "source": "Sample"
                },
                {
                    "city": "Tokyo", 
                    "country": "Japan", 
                    "latitude": 35.6762, 
                    "longitude": 139.6503,
                    "temperature": 28.7, 
                    "humidity": 68, 
                    "pressure": 1008.9,
                    "wind_speed": 3.5, 
                    "wind_direction": 180,
                    "description": "Clear sky", 
                    "timestamp": datetime.now().isoformat(),
                    "source": "Sample"
                }
            ]
            
            # Write sample data to a JSON file in the input directory
            sample_file_path = os.path.join(input_path, "sample_weather_data.json")
            with open(sample_file_path, 'w') as f:
                json.dump(sample_data, f)
            
            # Read the sample data
            df = spark.read.schema(schema).json(sample_file_path)
        else:
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
        
        # Sleep for a while to keep the job visible in the Spark UI
        logger.info("Processing complete. Waiting for 30 seconds to keep the job visible in the Spark UI...")
        import time
        time.sleep(30)
        
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
