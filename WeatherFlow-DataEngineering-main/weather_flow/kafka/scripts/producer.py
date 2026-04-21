"""Kafka producer for weather data streaming."""

import json
import logging
import time
import os
from typing import Dict, Any, List
import socket

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
KAFKA_TOPIC_RAW = "weather-raw-data"

# Add parent directory to path to import from scripts
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'scripts'))

# Import the weather API function
try:
    from weather_api import fetch_all_cities_weather
    logger.info("Successfully imported weather_api module")
except ImportError as e:
    logger.error(f"Failed to import weather_api module: {e}")
    sys.exit(1)

def check_kafka_connection():
    """Check if Kafka is accessible"""
    logger.info("Checking Kafka connection...")
    
    # Print network information
    try:
        logger.info(f"Hostname: {socket.gethostname()}")
        logger.info(f"IP Address: {socket.gethostbyname(socket.gethostname())}")
        
        # Try to resolve kafka hostname
        try:
            kafka_ip = socket.gethostbyname('kafka')
            logger.info(f"Kafka hostname resolves to: {kafka_ip}")
        except socket.gaierror:
            logger.error("Cannot resolve kafka hostname")
            return False
            
        # Try to connect to Kafka port
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            result = s.connect_ex(('kafka', 9092))
            if result == 0:
                logger.info("Kafka port 9092 is open")
                return True
            else:
                logger.error(f"Kafka port 9092 is not accessible, error code: {result}")
                return False
        finally:
            s.close()
    except Exception as e:
        logger.error(f"Error checking Kafka connection: {e}")
        return False

def create_producer(max_retries=5, retry_interval=5):
    """Create a Kafka producer with retry logic"""
    logger.info("Creating Kafka producer...")
    
    # Try multiple bootstrap server configurations
    bootstrap_servers = ['kafka:9092']
    logger.info(f"Using bootstrap servers: {bootstrap_servers}")
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries} to create Kafka producer")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=5,
                retry_backoff_ms=1000,
                request_timeout_ms=30000,
                connections_max_idle_ms=60000,
                client_id='weather-direct-producer'
            )
            logger.info("Successfully created Kafka producer")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Maximum retry attempts reached. Giving up.")
                raise

def send_weather_data(producer, data, topic=KAFKA_TOPIC_RAW):
    """Send weather data to Kafka topic"""
    try:
        # Create a key using city and timestamp
        key = f"{data.get('city', 'unknown')}_{data.get('timestamp', int(time.time()))}"
        
        # Send the data
        future = producer.send(topic, key=key, value=data)
        
        # Wait for the send to complete
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Sent data for {key} to {topic} | "
                  f"partition={record_metadata.partition}, "
                  f"offset={record_metadata.offset}")
        return True
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: {e}")
        return False

def stream_weather_data(interval_seconds: int = 300):
    """
    Stream weather data to Kafka at regular intervals.
    
    Args:
        interval_seconds: Interval between data fetches in seconds
    """
    try:
        # First check if Kafka is accessible
        if not check_kafka_connection():
            logger.error("Kafka is not accessible. Waiting 10 seconds before retrying...")
            time.sleep(10)
            if not check_kafka_connection():
                raise Exception("Kafka is not accessible after retry")
        
        # Create the Kafka producer
        producer = create_producer(max_retries=5, retry_interval=5)
        if not producer:
            raise Exception("Failed to create Kafka producer")
        
        logger.info("Starting weather data streaming...")
        
        while True:
            try:
                logger.info("Fetching weather data...")
                weather_data_list = fetch_all_cities_weather()
                
                if weather_data_list:
                    logger.info(f"Received {len(weather_data_list)} weather records")
                    success_count = 0
                    
                    for data in weather_data_list:
                        # Send each data point to Kafka
                        if send_weather_data(producer, data):
                            success_count += 1
                    
                    logger.info(f"Successfully sent {success_count}/{len(weather_data_list)} weather records to Kafka")
                else:
                    logger.warning("No weather data received from API")
                
                logger.info(f"Sleeping for {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error processing weather data batch: {e}")
                # Continue the loop instead of crashing
                logger.info(f"Sleeping for {interval_seconds} seconds before retrying...")
                time.sleep(interval_seconds)
                
    except KeyboardInterrupt:
        logger.info("Weather data streaming stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in weather data streaming: {e}")
    finally:
        # Ensure producer is closed if it exists
        if 'producer' in locals() and producer:
            try:
                producer.flush()
                producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")

if __name__ == "__main__":
    # Stream weather data every 5 minutes
    stream_weather_data(300)
