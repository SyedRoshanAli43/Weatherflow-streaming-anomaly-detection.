"""
Kafka monitor for WeatherFlow.
This script helps monitor the Kafka topics and messages for debugging.
"""

import json
import logging
import argparse
import sys
import os
from datetime import datetime
from typing import Dict, Any, List

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Add parent directory to path to import from scripts
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "scripts"))

from config_loader import get_config

_CFG = get_config()
_KAFKA_CFG = _CFG.get("kafka", {}) or {}
_TOPICS = _KAFKA_CFG.get("topics", {}) or {}

KAFKA_BOOTSTRAP_SERVERS = _KAFKA_CFG.get("bootstrap_servers", ["kafka:9092"])
KAFKA_BOOTSTRAP_SERVERS_STR = (
    ",".join(KAFKA_BOOTSTRAP_SERVERS)
    if isinstance(KAFKA_BOOTSTRAP_SERVERS, list)
    else str(KAFKA_BOOTSTRAP_SERVERS)
)
KAFKA_TOPIC_RAW = _TOPICS.get("raw", "weather-raw-data")
KAFKA_TOPIC_PROCESSED = _TOPICS.get("processed", "weather-processed-data")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_topics_exist(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
    """
    Ensure the required Kafka topics exist, create them if they don't.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers
        )
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        logger.info(f"Existing topics: {existing_topics}")
        
        # Topics to create
        topics_to_create = []
        
        # Check and add raw topic if it doesn't exist
        if KAFKA_TOPIC_RAW not in existing_topics:
            topics_to_create.append(NewTopic(
                name=KAFKA_TOPIC_RAW,
                num_partitions=1,
                replication_factor=1
            ))
        
        # Check and add processed topic if it doesn't exist
        if KAFKA_TOPIC_PROCESSED not in existing_topics:
            topics_to_create.append(NewTopic(
                name=KAFKA_TOPIC_PROCESSED,
                num_partitions=1,
                replication_factor=1
            ))
        
        # Create topics if any
        if topics_to_create:
            try:
                admin_client.create_topics(new_topics=topics_to_create)
                logger.info(f"Created topics: {[topic.name for topic in topics_to_create]}")
            except TopicAlreadyExistsError:
                logger.info("Topics already exist.")
        else:
            logger.info("All required topics already exist.")
        
        admin_client.close()
    except Exception as e:
        logger.error(f"Error ensuring topics exist: {e}")

def monitor_topic(topic, max_messages=10, timeout_ms=10000, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
    """
    Monitor a Kafka topic and print messages.
    
    Args:
        topic: Kafka topic to monitor
        max_messages: Maximum number of messages to consume
        timeout_ms: Timeout in milliseconds
        bootstrap_servers: Kafka bootstrap servers
    """
    try:
        logger.info(f"Monitoring topic: {topic}")
        
        # Create a consumer that starts from the latest offset
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f"monitor-{datetime.now().timestamp()}",  # Unique group ID
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=timeout_ms
        )
        
        # Consume messages
        message_count = 0
        
        print(f"\n{'-'*80}")
        print(f"Monitoring topic: {topic}")
        print(f"{'-'*80}\n")
        
        # Poll for new messages
        while message_count < max_messages:
            message_batch = consumer.poll(timeout_ms=1000, max_records=10)
            
            if not message_batch:
                print("No new messages. Waiting...")
                continue
            
            for partition, messages in message_batch.items():
                for message in messages:
                    message_count += 1
                    
                    print(f"\nMessage {message_count}:")
                    print(f"Partition: {partition.partition}")
                    print(f"Offset: {message.offset}")
                    print(f"Key: {message.key.decode('utf-8') if message.key else None}")
                    print(f"Timestamp: {datetime.fromtimestamp(message.timestamp/1000)}")
                    
                    # Pretty print the value
                    value = message.value
                    if isinstance(value, dict):
                        if 'raw_data' in value:
                            # Save raw_data separately to make output more readable
                            raw_data = value.pop('raw_data')
                            print("Value:")
                            print(json.dumps(value, indent=2))
                            print("Raw Data: [truncated for readability]")
                        else:
                            print("Value:")
                            print(json.dumps(value, indent=2))
                    else:
                        print(f"Value: {value}")
                    
                    print(f"{'-'*40}")
                    
                    if message_count >= max_messages:
                        break
                
                if message_count >= max_messages:
                    break
            
            # If we've reached max_messages, break out of the loop
            if message_count >= max_messages:
                break
        
        if message_count == 0:
            print("No messages received. The topic may be empty or no new messages are being produced.")
        
        # Close the consumer
        consumer.close()
        
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user.")
    except Exception as e:
        logger.error(f"Error monitoring topic {topic}: {e}")

def list_topics(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
    """
    List all topics in the Kafka cluster.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers
        )
        
        topics = admin_client.list_topics()
        
        print("\nKafka Topics:")
        print(f"{'-'*40}")
        for topic in topics:
            print(f"- {topic}")
        
        admin_client.close()
    except Exception as e:
        logger.error(f"Error listing topics: {e}")

def main():
    """Main function to monitor Kafka topics."""
    parser = argparse.ArgumentParser(description='Kafka Monitor for WeatherFlow')
    parser.add_argument('--topic', type=str, help='Topic to monitor')
    parser.add_argument('--max-messages', type=int, default=10, help='Maximum number of messages to consume')
    parser.add_argument('--timeout', type=int, default=30000, help='Timeout in milliseconds')
    parser.add_argument('--bootstrap-servers', type=str, default=KAFKA_BOOTSTRAP_SERVERS_STR, help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--list-topics', action='store_true', help='List all topics')
    parser.add_argument('--ensure-topics', action='store_true', help='Ensure required topics exist')
    
    args = parser.parse_args()
    bootstrap_servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
    
    # Ensure topics exist if requested
    if args.ensure_topics:
        ensure_topics_exist(bootstrap_servers)
    
    # List topics if requested
    if args.list_topics:
        list_topics(bootstrap_servers)
    
    # Monitor topic if specified
    if args.topic:
        monitor_topic(
            args.topic, 
            max_messages=args.max_messages, 
            timeout_ms=args.timeout, 
            bootstrap_servers=bootstrap_servers
        )
    
    # If no specific action was requested, show help
    if not (args.ensure_topics or args.list_topics or args.topic):
        parser.print_help()

if __name__ == "__main__":
    main()
