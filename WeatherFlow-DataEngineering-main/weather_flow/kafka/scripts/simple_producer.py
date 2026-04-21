#!/usr/bin/env python3
"""
Simple Kafka producer for weather data streaming.
This script directly connects to Kafka without complex class structures.
"""

import json
import logging
import time
import sys
import os
import socket
from typing import Dict, Any, List

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path to import from scripts
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'scripts'))

# Import the weather API function
try:
    from weather_api import fetch_all_cities_weather
    from config_loader import get_config
    logger.info("Successfully imported weather_api module")
except ImportError as e:
    logger.error(f"Failed to import weather_api module: {e}")
    sys.exit(1)

def main():
    """Main function to run the Kafka producer."""
    logger.info("Starting simple Kafka producer")
    cfg = get_config()
    kafka_cfg = cfg.get("kafka", {}) or {}
    topics = kafka_cfg.get("topics", {}) or {}
    topic_raw = topics.get("raw", "weather-raw-data")
    bootstrap_servers = kafka_cfg.get("bootstrap_servers", ["kafka:9092"])
    interval_seconds = int((cfg.get("ingestion", {}) or {}).get("interval_seconds", 300))
    
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
            
            # Create producer with explicit bootstrap servers
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=5,
                retry_backoff_ms=1000,
                request_timeout_ms=30000,
                connections_max_idle_ms=60000,
                client_id='simple-weather-producer'
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
    
    # Main loop to fetch and send weather data
    try:
        while True:
            try:
                logger.info("Fetching weather data...")
                weather_data_list = fetch_all_cities_weather()
                
                if weather_data_list:
                    logger.info(f"Fetched {len(weather_data_list)} weather records")
                    success_count = 0
                    
                    for data in weather_data_list:
                        try:
                            # Mark ingestion time for end-to-end latency measurements downstream.
                            data = dict(data)
                            data["ingest_ts_unix_ms"] = int(time.time() * 1000)

                            # Create a key using city name
                            key = f"{data.get('city', 'unknown')}"
                            
                            # Send data to Kafka
                            future = producer.send(topic_raw, key=key, value=data)
                            record_metadata = future.get(timeout=10)
                            
                            logger.info(f"Sent data for {key} -> {topic_raw} | "
                                      f"partition={record_metadata.partition}, "
                                      f"offset={record_metadata.offset}")
                            
                            success_count += 1
                        except Exception as e:
                            logger.error(f"Error sending data for {data.get('city', 'unknown')}: {e}")
                    
                    logger.info(f"Successfully sent {success_count}/{len(weather_data_list)} weather records")
                else:
                    logger.warning("No weather data received")
                
                logger.info(f"Sleeping for {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.info(f"Sleeping for {interval_seconds} seconds before retrying...")
                time.sleep(interval_seconds)
                
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        if 'producer' in locals():
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()
