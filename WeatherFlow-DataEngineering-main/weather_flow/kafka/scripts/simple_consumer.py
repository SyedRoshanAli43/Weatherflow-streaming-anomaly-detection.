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
from pathlib import Path
from typing import Dict, Any, List

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path to import shared modules under `weather_flow/scripts`
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'scripts'))

try:
    from config_loader import get_config, get_path
    from data_quality import DataQualityMonitor
    from anomaly_detection import (
        RollingZScoreDetector,
        IsolationForestDetector,
        AnomalyEventLogger,
    )
    from metrics_logger import MetricsLogger
except ImportError as e:
    logger.error(f"Failed to import WeatherFlow monitoring modules: {e}")
    raise

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
    """Main function to run the Kafka consumer."""
    logger.info("Starting simple Kafka consumer")

    cfg = get_config()
    kafka_cfg = cfg.get("kafka", {}) or {}
    topics = kafka_cfg.get("topics", {}) or {}
    topic_raw = topics.get("raw", "weather-raw-data")
    bootstrap_servers = kafka_cfg.get("bootstrap_servers", ["kafka:9092"])

    monitoring_cfg = cfg.get("monitoring", {}) or {}
    log_every_n = int(monitoring_cfg.get("log_every_n_messages", 25))

    processed_path = get_path(cfg, "processed_dir", "data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Processed data will be saved to: {processed_path}")

    # Monitoring sinks
    metrics_path = get_path(cfg, "monitoring_dir", "data/monitoring") / Path(
        monitoring_cfg.get("metrics_file", "data/monitoring/stream_metrics.jsonl")
    ).name
    quality_path = get_path(cfg, "monitoring_dir", "data/monitoring") / Path(
        monitoring_cfg.get("quality_events_file", "data/monitoring/data_quality_events.jsonl")
    ).name
    anomaly_path = get_path(cfg, "monitoring_dir", "data/monitoring") / Path(
        monitoring_cfg.get("anomaly_events_file", "data/monitoring/anomaly_events.jsonl")
    ).name

    metrics = MetricsLogger(metrics_path=metrics_path, log_every_n=log_every_n)
    dq_cfg = cfg.get("data_quality", {}) or {}
    dq = DataQualityMonitor(
        required_fields=list(dq_cfg.get("required_fields", [])),
        bounds=dict(dq_cfg.get("bounds", {}) or {}),
        quality_events_path=quality_path,
    )

    ad_cfg = cfg.get("anomaly_detection", {}) or {}
    features = list(ad_cfg.get("features", ["temperature", "humidity", "pressure", "wind_speed"]))
    method = str(ad_cfg.get("method", "zscore")).lower()

    zc = ad_cfg.get("zscore", {}) or {}
    z_detector = RollingZScoreDetector(
        features=features,
        window_size=int(zc.get("window_size", 240)),
        threshold=float(zc.get("threshold", 3.5)),
        min_samples=int(zc.get("min_samples", 30)),
    )

    if_cfg = ad_cfg.get("isolation_forest", {}) or {}
    if_enabled = bool(if_cfg.get("enabled", False))
    if_detector = None
    if if_enabled:
        try:
            if_detector = IsolationForestDetector(
                features=features,
                contamination=float(if_cfg.get("contamination", 0.01)),
                min_samples=int(if_cfg.get("min_samples", 200)),
            )
        except Exception as e:
            logger.warning(f"IsolationForest disabled due to import/setup error: {e}")
            if_detector = None

    anomaly_logger = AnomalyEventLogger(anomaly_events_path=anomaly_path)
    
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
                topic_raw,
                bootstrap_servers=bootstrap_servers,
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
        
        logger.info(f"Starting to consume messages from topic {topic_raw}")
        
        for message in consumer:
            try:
                # Extract message data
                key = message.key
                data = message.value
                
                logger.info(f"Received message: key={key}, partition={message.partition}, offset={message.offset}")
                
                # Process the data
                processed_data = process_weather_data(data)

                # Data quality checks on the *processed* view (post normalization)
                issues = dq.validate(processed_data or {})
                dq.emit(processed_data or {}, issues)

                # Anomaly detection (feature-level z-scores or IsolationForest)
                anomaly_result = None
                if method == "isolation_forest" and if_detector is not None:
                    anomaly_result = if_detector.score(processed_data or {})
                else:
                    anomaly_result = z_detector.score(processed_data or {})

                if processed_data is not None:
                    processed_data = dict(processed_data)
                    processed_data["data_quality"] = {
                        "passed": len(issues) == 0,
                        "issue_count": len(issues),
                    }
                    processed_data["anomaly"] = anomaly_result.to_dict() if anomaly_result else None

                if anomaly_result is not None:
                    anomaly_logger.emit(processed_data or {}, anomaly_result)

                # Update throughput + latency metrics
                metrics.observe(
                    processed_data or {},
                    kafka_partition=message.partition,
                    kafka_offset=message.offset,
                )
                
                # Save processed data
                if processed_data:
                    city = processed_data.get('city', 'unknown').lower().replace(' ', '_')
                    timestamp = int(time.time())
                    filename = processed_path / f"weather_{city}_{timestamp}.json"
                    
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
        try:
            metrics.flush()
        except Exception:
            pass
        
        logger.info(f"Consumer session summary: Processed {message_count} messages with {error_count} errors")

if __name__ == "__main__":
    main()
