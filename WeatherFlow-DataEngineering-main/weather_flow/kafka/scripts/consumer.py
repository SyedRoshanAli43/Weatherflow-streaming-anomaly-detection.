"""
Kafka consumer for weather data processing.
"""

import json
import logging
import time
import os
import time
import sys
import socket
from typing import Dict, Any, Callable, List

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
KAFKA_TOPIC_PROCESSED = "weather-processed-data"

# Path for saving processed data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed')

# Create the processed data directory if it doesn't exist
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

logger.info(f"Processed data will be saved to: {PROCESSED_DATA_PATH}")

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'scripts'))

def check_kafka_connection():
    """Check if Kafka is accessible"""
    logger.info("Checking Kafka connection from consumer...")
    
    # Print network information
    try:
        logger.info(f"Consumer hostname: {socket.gethostname()}")
        logger.info(f"Consumer IP Address: {socket.gethostbyname(socket.gethostname())}")
        
        # Try to resolve kafka hostname
        try:
            kafka_ip = socket.gethostbyname('kafka')
            logger.info(f"Kafka hostname resolves to: {kafka_ip} from consumer")
        except socket.gaierror:
            logger.error("Cannot resolve kafka hostname from consumer")
            return False
            
        # Try to connect to Kafka port
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            result = s.connect_ex(('kafka', 9092))
            if result == 0:
                logger.info("Kafka port 9092 is open from consumer")
                return True
            else:
                logger.error(f"Kafka port 9092 is not accessible from consumer, error code: {result}")
                return False
        finally:
            s.close()
    except Exception as e:
        logger.error(f"Error checking Kafka connection from consumer: {e}")
        return False

def create_consumer(topic=KAFKA_TOPIC_RAW, group_id="weather-consumer-group", max_retries=5, retry_interval=5):
    """Create a Kafka consumer with retry logic"""
    logger.info(f"Creating Kafka consumer for topic {topic}...")
    
    # Try multiple bootstrap server configurations
    bootstrap_servers = ['kafka:9092']
    logger.info(f"Using bootstrap servers: {bootstrap_servers}")
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries} to create Kafka consumer")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                request_timeout_ms=30000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                client_id='weather-direct-consumer'
            )
            logger.info("Successfully created Kafka consumer")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error("Maximum retry attempts reached. Giving up.")
                raise

def process_and_save_message(message, process_func):
    """Process a Kafka message and save the result"""
    try:
        # Extract message data
        key = message.key
        data = message.value
        
        logger.info(f"Processing message: key={key}, partition={message.partition}, offset={message.offset}")
        
        # Process the data
        if process_func and callable(process_func):
            processed_data = process_func(data)
            
            # Save processed data
            if processed_data:
                save_processed_data(processed_data)
                return True
        
        return False
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False

def save_processed_data(data):
    """Save processed data to a file"""
    try:
        city = data.get('city', 'unknown').lower().replace(' ', '_')
        source = data.get('source', 'unknown')
        timestamp = data.get('processed_timestamp', '').replace(':', '-').replace('.', '-')
        
        if not timestamp:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        filename = f"{PROCESSED_DATA_PATH}/processed_{source}_{city}_{timestamp}.json"
        
        save_to_json(data, filename)
        logger.info(f"Saved processed data to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving processed data: {e}")
        return False

def save_to_json(data, filename):
    """Save data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")
        return False

# Logger already initialized above

class WeatherConsumer:
    """Kafka consumer for weather data."""
    
    def __init__(self, 
                 topic: str = KAFKA_TOPIC_RAW, 
                 bootstrap_servers: str = None,
                 group_id: str = "weather-consumer-group"):
        """
        Initialize the Kafka consumer.
        
        Args:
            topic: Kafka topic to consume from
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
        """
        # Force using kafka:9092 instead of relying on environment variables
        self.bootstrap_servers = 'kafka:9092'
        logger.info(f"Consumer using bootstrap servers: {self.bootstrap_servers}")
        self.topic = topic
        self.group_id = group_id
        self.consumer = self._create_consumer()

    def _create_consumer(self) -> KafkaConsumer:
        """
        Create and return a Kafka consumer instance.
        
        Returns:
            KafkaConsumer: Configured Kafka consumer
        """
        try:
            # Use a list format for bootstrap_servers to ensure proper parsing
            bootstrap_servers = ['kafka:9092']
            logger.info(f"Creating consumer with bootstrap servers: {bootstrap_servers}")
            return KafkaConsumer(
                self.topic,
                bootstrap_servers=bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                client_id='weather-consumer-client'
            )
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise

    def consume_messages(self, 
                         process_func: Callable[[Dict[str, Any]], Dict[str, Any]] = None,
                         save_processed: bool = True):
        """
        Consume and process messages from Kafka.
        
        Args:
            process_func: Function to process messages
            save_processed: Whether to save processed data locally
        """
        try:
            logger.info(f"Starting to consume messages from topic: {self.topic}")
            
            for message in self.consumer:
                try:
                    # Get the message key and value
                    key = message.key
                    data = message.value
                    
                    logger.info(f"Received message: key={key}, partition={message.partition}, "
                              f"offset={message.offset}")
                    
                    # Process the data if a processing function is provided
                    if process_func and callable(process_func):
                        processed_data = process_func(data)
                        
                        # Save processed data if requested
                        if save_processed and processed_data:
                            self._save_processed_data(processed_data)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        except KeyboardInterrupt:
            logger.info("Consumption stopped by user")
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            self.close()

    def _save_processed_data(self, data: Dict[str, Any]):
        """
        Save processed data locally.
        
        Args:
            data: Processed data to save
        """
        try:
            city = data.get('city', 'unknown').lower().replace(' ', '_')
            source = data.get('source', 'unknown')
            timestamp = data.get('processed_timestamp', '').replace(':', '-').replace('.', '-')
            
            if not timestamp:
                timestamp = time.strftime("%Y%m%d_%H%M%S")
            
            filename = f"{PROCESSED_DATA_PATH}/processed_{source}_{city}_{timestamp}.json"
            
            save_to_json(data, filename)
            logger.info(f"Saved processed data to {filename}")
        
        except Exception as e:
            logger.error(f"Error saving processed data: {e}")

    def close(self):
        """Close the Kafka consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

def basic_weather_processing(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Basic processing for weather data.
    
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
            
            # Convert back to Celsius
            processed['heat_index_fahrenheit'] = heat_index
            processed['heat_index_celsius'] = (heat_index - 32) * 5/9
    
    # Add weather severity level based on conditions
    if 'weather_condition' in processed:
        condition = processed['weather_condition'].lower()
        
        # Define severity levels for different weather conditions
        severe_conditions = ['thunderstorm', 'tornado', 'hurricane', 'blizzard']
        moderate_conditions = ['rain', 'snow', 'drizzle', 'fog', 'mist', 'shower']
        mild_conditions = ['cloudy', 'overcast', 'partly cloudy']
        
        if any(cond in condition for cond in severe_conditions):
            severity = 'severe'
        elif any(cond in condition for cond in moderate_conditions):
            severity = 'moderate'
        elif any(cond in condition for cond in mild_conditions):
            severity = 'mild'
        else:
            severity = 'normal'
        
        processed['weather_severity'] = severity
    
    return processed

def start_consumer():
    """Start the weather data consumer."""
    try:
        # First check if Kafka is accessible
        if not check_kafka_connection():
            logger.error("Kafka is not accessible from consumer. Waiting 10 seconds before retrying...")
            time.sleep(10)
            if not check_kafka_connection():
                raise Exception("Kafka is not accessible from consumer after retry")
        
        # Create the Kafka consumer
        consumer = create_consumer(
            topic=KAFKA_TOPIC_RAW, 
            group_id="weather-consumer-group",
            max_retries=5, 
            retry_interval=5
        )
        
        if not consumer:
            raise Exception("Failed to create Kafka consumer")
        
        logger.info(f"Starting to consume messages from topic {KAFKA_TOPIC_RAW}")
        
        # Process messages in a loop with error handling
        message_count = 0
        error_count = 0
        max_consecutive_errors = 5
        consecutive_errors = 0
        
        try:
            for message in consumer:
                try:
                    # Process and save the message
                    if process_and_save_message(message, basic_weather_processing):
                        message_count += 1
                        consecutive_errors = 0  # Reset consecutive errors counter on success
                        
                        # Log progress periodically
                        if message_count % 10 == 0:
                            logger.info(f"Successfully processed {message_count} messages so far")
                    else:
                        error_count += 1
                        consecutive_errors += 1
                        
                    # Check if we've had too many consecutive errors
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}). Restarting consumer...")
                        break
                        
                except Exception as e:
                    error_count += 1
                    consecutive_errors += 1
                    logger.error(f"Error processing message: {e}")
                    
                    # Check if we've had too many consecutive errors
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}). Restarting consumer...")
                        break
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            # Close the consumer properly
            try:
                consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
                
        logger.info(f"Consumer session summary: Processed {message_count} messages with {error_count} errors")
        
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        # Don't raise the exception, just log it and return
        return False
        
    return True

if __name__ == "__main__":
    start_consumer()
