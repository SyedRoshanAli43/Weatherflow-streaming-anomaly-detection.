#!/usr/bin/env python3
"""
Simple Kafka consumer for weather data processing.
This script directly connects to Kafka without complex class structures.
"""

import json
import logging
import time
import sys
import os
import socket
from typing import Dict, Any, List

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
KAFKA_TOPIC_RAW = "weather-raw-data"

# Path for saving processed data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed')

# Create the processed data directory if it doesn't exist
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
logger.info(f"Processed data will be saved to: {PROCESSED_DATA_PATH}")

def save_to_json(data, filename):
    """Save data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Data saved to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")
        return False

def process_weather_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process weather data with basic transformations.
    
    Args:
        data: Raw weather data
        
    Returns:
        Dict: Processed weather data
    """
    # Copy the original data
    processed = data.copy()
    
    # Add a processed timestamp
    processed['processed_timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Convert temperature to Fahrenheit if it's in Celsius
    if 'temperature' in processed:
        temp_c = processed['temperature']
        processed['temperature_fahrenheit'] = (temp_c * 9/5) + 32
    
    # Calculate heat index if temperature and humidity are available
    if 'temperature' in processed and 'humidity' in processed:
        temp_c = processed['temperature']
        humidity = processed['humidity']
        temp_f = (temp_c * 9/5) + 32
        
        # Simple heat index formula (for temperatures > 80°F)
        if temp_f > 80:
            heat_index = -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity
            heat_index += -0.22475541 * temp_f * humidity - 6.83783e-3 * temp_f**2
            heat_index += -5.481717e-2 * humidity**2 + 1.22874e-3 * temp_f**2 * humidity
            heat_index += 8.5282e-4 * temp_f * humidity**2 - 1.99e-6 * temp_f**2 * humidity**2
            processed['heat_index_fahrenheit'] = heat_index
            processed['heat_index_celsius'] = (heat_index - 32) * 5/9
    
    # Determine weather severity based on conditions
    severity = 'normal'
    
    # Check temperature extremes
    if 'temperature' in processed:
        if processed['temperature'] > 35:  # Very hot
            severity = 'high'
        elif processed['temperature'] < -10:  # Very cold
            severity = 'high'
        elif processed['temperature'] > 30 or processed['temperature'] < 0:
            severity = 'moderate'
    
    # Check for severe weather conditions in description
    if 'description' in processed:
        severe_conditions = ['storm', 'hurricane', 'tornado', 'blizzard', 'flood', 'severe']
        moderate_conditions = ['rain', 'snow', 'wind', 'fog', 'mist', 'drizzle']
        
        desc = processed['description'].lower()
        
        for condition in severe_conditions:
            if condition in desc:
                severity = 'high'
                break
                
        if severity != 'high':  # Only check moderate if not already high
            for condition in moderate_conditions:
                if condition in desc:
                    severity = max(severity, 'moderate')  # Only upgrade to moderate if not already high
                    break
        
        processed['weather_severity'] = severity
    
    return processed

def main():
    """Main function to run the Kafka consumer"""
    logger.info("Starting simple Kafka consumer")
    
    # Print environment information
    logger.info(f"Environment variables: {os.environ}")
    logger.info(f"Hostname: {socket.gethostname()}")
    
    # Try to resolve Kafka hostname
    try:
        kafka_ip = socket.gethostbyname('kafka')
        logger.info(f"Kafka hostname resolves to: {kafka_ip}")
    except socket.gaierror as e:
        logger.error(f"Cannot resolve kafka hostname: {e}")
        # Continue anyway, we'll try direct connection
    
    # Connect to Kafka
    max_retries = 10
    retry_interval = 5
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries} to connect to Kafka")
            
            # Create consumer with explicit bootstrap servers
            # Fix: Make sure session_timeout_ms is less than request_timeout_ms
            consumer = KafkaConsumer(
                KAFKA_TOPIC_RAW,
                bootstrap_servers=['kafka:9092'],
                group_id='simple-weather-consumer-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                request_timeout_ms=60000,  # Increased to 60 seconds
                session_timeout_ms=30000,  # Kept at 30 seconds (less than request_timeout)
                heartbeat_interval_ms=10000,
                client_id='simple-weather-consumer'
            )
            
            logger.info("Successfully connected to Kafka!")
            break
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Maximum retry attempts reached. Exiting.")
                sys.exit(1)
    
    # Main loop to consume and process messages
    try:
        message_count = 0
        error_count = 0
        
        logger.info(f"Starting to consume messages from topic {KAFKA_TOPIC_RAW}")
        
        for message in consumer:
            try:
                # Extract message data
                key = message.key
                data = message.value
                
                logger.info(f"Received message: key={key}, partition={message.partition}, offset={message.offset}")
                
                # Process the data
                processed_data = process_weather_data(data)
                
                # Save processed data
                if processed_data:
                    city = processed_data.get('city', 'unknown').lower().replace(' ', '_')
                    timestamp = int(time.time())
                    filename = os.path.join(PROCESSED_DATA_PATH, f"weather_{city}_{timestamp}.json")
                    
                    if save_to_json(processed_data, filename):
                        message_count += 1
                        
                        # Log progress periodically
                        if message_count % 10 == 0:
                            logger.info(f"Successfully processed {message_count} messages so far")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logger.info("Kafka consumer closed")
        
        logger.info(f"Consumer session summary: Processed {message_count} messages with {error_count} errors")

if __name__ == "__main__":
    main()
