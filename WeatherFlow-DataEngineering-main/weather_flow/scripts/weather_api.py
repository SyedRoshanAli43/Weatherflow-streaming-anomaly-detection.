"""
Weather API data fetcher for WeatherFlow.
"""

import time
import logging
import json
from typing import Dict, Any, List
import os
from datetime import datetime

import requests
# Add parent directory to path to import from scripts
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Local config (YAML + env overrides) to avoid hardcoded constants.
try:
    from config_loader import get_config, get_path
except ImportError:  # pragma: no cover
    get_config = None  # type: ignore
    get_path = None  # type: ignore

# Define paths directly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')

# Create the data directory if it doesn't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)

# API Keys from environment variables
OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY', 'your_openweather_api_key')
WEATHERAPI_KEY = os.environ.get('WEATHERAPI_KEY', 'your_weatherapi_key')

# API URLs
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
WEATHERAPI_URL = "https://api.weatherapi.com/v1/current.json"

# Cities to fetch weather data for
CITIES = [
    {"name": "New York", "country": "US"},
    {"name": "London", "country": "UK"},
    {"name": "Tokyo", "country": "Japan"},
    {"name": "Berlin", "country": "Germany"},
    {"name": "Mexico City", "country": "Mexico"}
]

# Override defaults from config.yaml when available.
if get_config is not None:
    cfg = get_config()
    try:
        RAW_DATA_PATH = str(get_path(cfg, "raw_dir", "data/raw"))  # type: ignore[arg-type]
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
    except Exception:
        pass

    src = cfg.get("sources", {}) or {}
    OPENWEATHER_URL = (src.get("openweather", {}) or {}).get("base_url", OPENWEATHER_URL)
    WEATHERAPI_URL = (src.get("weatherapi", {}) or {}).get("base_url", WEATHERAPI_URL)

    cfg_cities = cfg.get("cities")
    if isinstance(cfg_cities, list) and cfg_cities:
        CITIES = cfg_cities
# Define utility functions directly
def make_api_request(url, params):
    """Make an API request and return the response."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for 4XX/5XX responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        return None

def parse_weather_data(data, source, city):
    """Parse weather data from API response."""
    if not data:
        return None
    
    # Different parsing logic based on the source
    if source == "openweathermap":
        # Parse OpenWeatherMap data
        return {
            "timestamp": datetime.now().isoformat(),
            "source": "openweathermap",
            "city": city["name"],
            "country": city["country"],
            "coordinates": {
                "lat": data.get("coord", {}).get("lat"),
                "lon": data.get("coord", {}).get("lon")
            },
            "temperature": data.get("main", {}).get("temp") - 273.15,  # Convert from Kelvin to Celsius
            "humidity": data.get("main", {}).get("humidity"),
            "pressure": data.get("main", {}).get("pressure"),
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_direction": data.get("wind", {}).get("deg"),
            "clouds": data.get("clouds", {}).get("all"),
            "weather_condition": data.get("weather", [{}])[0].get("main"),
            "weather_description": data.get("weather", [{}])[0].get("description")
        }
    elif source == "weatherapi":
        # Parse WeatherAPI data
        return {
            "timestamp": datetime.now().isoformat(),
            "source": "weatherapi",
            "city": city["name"],
            "country": city["country"],
            "coordinates": {
                "lat": data.get("location", {}).get("lat"),
                "lon": data.get("location", {}).get("lon")
            },
            "temperature": data.get("current", {}).get("temp_c"),
            "humidity": data.get("current", {}).get("humidity"),
            "pressure": data.get("current", {}).get("pressure_mb"),
            "wind_speed": data.get("current", {}).get("wind_kph") / 3.6,  # Convert from km/h to m/s
            "wind_direction": data.get("current", {}).get("wind_degree"),
            "clouds": data.get("current", {}).get("cloud"),
            "weather_condition": data.get("current", {}).get("condition", {}).get("text"),
            "weather_description": data.get("current", {}).get("condition", {}).get("text")
        }
    else:
        return None

def save_to_json(data, filename):
    """Save data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        logging.error(f"Error saving data to {filename}: {e}")
        return False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_openweather_data(city: dict) -> Dict[str, Any]:
    """
    Fetch weather data from OpenWeatherMap API.
    
    Args:
        city: Dictionary containing city name and country
        
    Returns:
        Dict: Weather data
    """
    city_query = f"{city['name']},{city['country']}"
    params = {
        'q': city_query,
        'appid': OPENWEATHER_API_KEY,
        'units': 'metric'
    }
    
    data = make_api_request(OPENWEATHER_URL, params)
    if data:
        return parse_weather_data(data, 'openweathermap', city)
    return None

def fetch_weatherapi_data(city: dict) -> Dict[str, Any]:
    """
    Fetch weather data from WeatherAPI.
    
    Args:
        city: Dictionary containing city name and country
        
    Returns:
        Dict: Weather data
    """
    city_query = f"{city['name']},{city['country']}"
    params = {
        'q': city_query,
        'key': WEATHERAPI_KEY
    }
    
    data = make_api_request(WEATHERAPI_URL, params)
    if data:
        return parse_weather_data(data, 'weatherapi', city)
    return None

def fetch_all_cities_weather() -> List[Dict[str, Any]]:
    """
    Fetch weather data for all configured cities.
    
    Returns:
        List: Weather data for all cities
    """
    all_weather_data = []
    
    for city in CITIES:
        logger.info(f"Fetching weather data for {city}")
        
        # Try OpenWeatherMap first
        openweather_data = fetch_openweather_data(city)
        if openweather_data:
            all_weather_data.append(openweather_data)
        
        # Then try WeatherAPI
        weatherapi_data = fetch_weatherapi_data(city)
        if weatherapi_data:
            all_weather_data.append(weatherapi_data)
        
        # Avoid hitting API rate limits
        time.sleep(1)
    
    return all_weather_data

def save_weather_data_locally(weather_data: List[Dict[str, Any]]):
    """
    Save weather data to local files for backup/processing.
    
    Args:
        weather_data: List of weather data dictionaries
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for data in weather_data:
        city = data.get('city', 'unknown').lower().replace(' ', '_')
        source = data.get('source', 'unknown')
        filename = f"{RAW_DATA_PATH}/{source}_{city}_{timestamp}.json"
        
        save_to_json(data, filename)
        logger.info(f"Saved weather data to {filename}")

def main():
    """Main function to fetch and save weather data."""
    try:
        logger.info("Starting weather data collection")
        weather_data = fetch_all_cities_weather()
        logger.info(f"Collected data for {len(weather_data)} city-source combinations")
        
        # Save to local files
        save_weather_data_locally(weather_data)
        
        logger.info("Weather data collection completed successfully")
        return weather_data
    except Exception as e:
        logger.error(f"Error in weather data collection: {e}")
        return []

if __name__ == "__main__":
    main()
