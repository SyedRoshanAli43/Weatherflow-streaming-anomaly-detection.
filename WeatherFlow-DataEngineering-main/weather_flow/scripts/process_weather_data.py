#!/usr/bin/env python3
"""
Python script to process weather data without requiring Spark.
This script is a simplified version that can run directly in the Airflow container.
"""

import os
import sys
import json
import argparse
from datetime import datetime
import logging
import glob
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_weather_data(input_path, output_path):
    """
    Process weather data using pandas instead of Spark.
    
    Args:
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
    
    # Get all JSON files in the input directory
    json_files = glob.glob(os.path.join(input_path, "*.json"))
    
    if not json_files:
        logger.warning("No JSON files found to process")
        return
    
    # Read and combine all JSON files
    all_data = []
    for file in json_files:
        try:
            with open(file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
        except Exception as e:
            logger.error(f"Error reading file {file}: {e}")
    
    if not all_data:
        logger.warning("No data found to process")
        return
    
    logger.info(f"Loaded {len(all_data)} records")
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(all_data)
    
    # Basic transformations
    df['temperature_fahrenheit'] = (df['temperature'] * 9/5) + 32
    df['processed_timestamp'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Calculate heat index for high temperatures
    def calculate_heat_index(row):
        if row['temperature_fahrenheit'] > 80:
            t = row['temperature_fahrenheit']
            h = row['humidity']
            return -42.379 + 2.04901523*t + 10.14333127*h - 0.22475541*t*h - 0.00683783*t*t - \
                   0.05481717*h*h + 0.00122874*t*t*h + 0.00085282*t*h*h - 0.00000199*t*t*h*h
        return None
    
    df['heat_index_fahrenheit'] = df.apply(calculate_heat_index, axis=1)
    df['heat_index_celsius'] = (df['heat_index_fahrenheit'] - 32) * 5/9
    
    # Determine weather severity
    def determine_severity(row):
        t = row['temperature']
        if t > 35 or t < -10:
            return "high"
        elif (t > 30 and t <= 35) or (t < 0 and t >= -10):
            return "moderate"
        else:
            return "normal"
    
    df['weather_severity'] = df.apply(determine_severity, axis=1)
    
    # Save processed data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_json = os.path.join(output_path, f"weather_processed_{timestamp}.json")
    output_file_csv = os.path.join(output_path, f"weather_processed_{timestamp}.csv")
    
    # Save as JSON
    df.to_json(output_file_json, orient='records', lines=True)
    
    # Save as CSV (more efficient for analytics)
    df.to_csv(output_file_csv, index=False)
    
    logger.info(f"Successfully processed and saved data to {output_file_json} and {output_file_csv}")
    
    # Show some statistics
    logger.info("Weather data statistics:")
    logger.info(df[['temperature', 'humidity', 'pressure']].describe())
    
    # Count by city
    logger.info("Records by city:")
    logger.info(df.groupby('city').size())

def main():
    """Main function to run the processor."""
    parser = argparse.ArgumentParser(description='Process weather data')
    parser.add_argument('--input', required=True, help='Input directory with raw weather data')
    parser.add_argument('--output', required=True, help='Output directory for processed data')
    
    # If running with arguments, get them from sys.argv
    if len(sys.argv) > 1:
        args = parser.parse_args()
        input_path = args.input
        output_path = args.output
    else:
        # Default paths for testing
        input_path = "/opt/airflow/data/raw"
        output_path = "/opt/airflow/data/processed"
    
    logger.info(f"Starting weather data processing from {input_path} to {output_path}")
    
    try:
        # Process data
        process_weather_data(input_path, output_path)
        
        logger.info("Weather data processing completed successfully")
    except Exception as e:
        logger.error(f"Failed to process weather data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
