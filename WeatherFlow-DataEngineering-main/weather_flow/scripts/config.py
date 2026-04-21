"""
Configuration settings for the WeatherFlow application.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

from config_loader import get_config, get_path

# Load environment variables from .env file
load_dotenv()

_CFG = get_config()

# API Configuration (secrets via env vars; never hardcode in repo)
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "your_openweather_api_key")
WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY", "your_weatherapi_key")

# API URLs (configurable)
OPENWEATHER_URL = ((_CFG.get("sources", {}) or {}).get("openweather", {}) or {}).get(
    "base_url", "https://api.openweathermap.org/data/2.5/weather"
)
WEATHERAPI_URL = ((_CFG.get("sources", {}) or {}).get("weatherapi", {}) or {}).get(
    "base_url", "https://api.weatherapi.com/v1/current.json"
)

# List of cities to monitor
_cities_cfg = _CFG.get("cities")
CITIES = (
    [c.get("name") for c in _cities_cfg if isinstance(c, dict) and c.get("name")]
    if isinstance(_cities_cfg, list) and _cities_cfg
    else [
        "London",
        "New York",
        "Tokyo",
        "Sydney",
        "Paris",
        "Berlin",
        "Moscow",
        "Beijing",
        "Rio de Janeiro",
        "Cairo",
    ]
)

# Kafka Configuration
_kafka_cfg = _CFG.get("kafka", {}) or {}
KAFKA_BOOTSTRAP_SERVERS = ",".join(_kafka_cfg.get("bootstrap_servers", ["localhost:9092"]))
KAFKA_TOPIC_RAW = ((_kafka_cfg.get("topics", {}) or {}).get("raw")) or "weather-raw-data"
KAFKA_TOPIC_PROCESSED = ((_kafka_cfg.get("topics", {}) or {}).get("processed")) or "weather-processed-data"

# Spark Configuration
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DB = "weatherflow"
MONGODB_COLLECTION_RAW = "weather_raw"
MONGODB_COLLECTION_PROCESSED = "weather_processed"

# Data paths (configurable)
DATA_PATH = str(get_path(_CFG, "data_dir", "data"))
RAW_DATA_PATH = str(get_path(_CFG, "raw_dir", "data/raw"))
PROCESSED_DATA_PATH = str(get_path(_CFG, "processed_dir", "data/processed"))

# Ensure data directories exist
Path(RAW_DATA_PATH).mkdir(parents=True, exist_ok=True)
Path(PROCESSED_DATA_PATH).mkdir(parents=True, exist_ok=True)

# Dashboard Configuration
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
