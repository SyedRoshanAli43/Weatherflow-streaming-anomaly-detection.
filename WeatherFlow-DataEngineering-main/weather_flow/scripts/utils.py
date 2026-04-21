"""
Utility functions for the WeatherFlow application.
"""

import os
import json
import logging
import datetime
import requests
from typing import Dict, Any, Optional, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def save_to_json(data: Dict[str, Any], filename: str) -> bool:
    """
    Save data to a JSON file.
    
    Args:
        data: The data to save
        filename: The filename to save to
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")
        return False

def make_api_request(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Make an API request and return the response.
    
    Args:
        url: The URL to request
        params: The parameters to include in the request
        
    Returns:
        The JSON response or None if the request failed
    """
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request to {url}: {e}")
        return None

def get_timestamp() -> str:
    """
    Get the current timestamp in ISO format.
    
    Returns:
        str: The current timestamp
    """
    return datetime.datetime.now().isoformat()

def parse_weather_data(data: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    Parse weather data into a standardized format.
    
    Args:
        data: The raw weather data
        source: The source of the data (e.g., 'openweather', 'weatherapi')
        
    Returns:
        Dict: Standardized weather data
    """
    timestamp = get_timestamp()
    
    if source == 'openweather':
        return {
            'timestamp': timestamp,
            'source': source,
            'city': data.get('name', 'Unknown'),
            'country': data.get('sys', {}).get('country', 'Unknown'),
            'temperature': data.get('main', {}).get('temp', 0),
            'humidity': data.get('main', {}).get('humidity', 0),
            'pressure': data.get('main', {}).get('pressure', 0),
            'wind_speed': data.get('wind', {}).get('speed', 0),
            'wind_direction': data.get('wind', {}).get('deg', 0),
            'weather_condition': data.get('weather', [{}])[0].get('main', 'Unknown'),
            'weather_description': data.get('weather', [{}])[0].get('description', 'Unknown'),
            'clouds': data.get('clouds', {}).get('all', 0),
            'coordinates': {
                'lat': data.get('coord', {}).get('lat', 0),
                'lon': data.get('coord', {}).get('lon', 0)
            },
            'raw_data': data
        }
    elif source == 'weatherapi':
        current = data.get('current', {})
        location = data.get('location', {})
        return {
            'timestamp': timestamp,
            'source': source,
            'city': location.get('name', 'Unknown'),
            'country': location.get('country', 'Unknown'),
            'temperature': current.get('temp_c', 0),
            'humidity': current.get('humidity', 0),
            'pressure': current.get('pressure_mb', 0),
            'wind_speed': current.get('wind_kph', 0),
            'wind_direction': current.get('wind_degree', 0),
            'weather_condition': current.get('condition', {}).get('text', 'Unknown'),
            'weather_description': current.get('condition', {}).get('text', 'Unknown'),
            'clouds': current.get('cloud', 0),
            'coordinates': {
                'lat': location.get('lat', 0),
                'lon': location.get('lon', 0)
            },
            'raw_data': data
        }
    else:
        logger.warning(f"Unknown source: {source}")
        return {
            'timestamp': timestamp,
            'source': source,
            'raw_data': data
        }
