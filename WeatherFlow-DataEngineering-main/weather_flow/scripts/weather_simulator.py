"""
Weather Station Simulator for WeatherFlow.
Generates synthetic weather data to simulate IoT weather stations.
"""

import os
import sys
import json
import time
import random
import logging
from datetime import datetime, timedelta
import argparse
from typing import Dict, Any, List

# Define paths directly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')

# Create the data directory if it doesn't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)

# Define save_to_json function here for self-containment
def save_to_json(data, filename):
    """Save data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")
        return False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define weather station locations
WEATHER_STATIONS = [
    {"id": "WS001", "name": "Central Park", "city": "New York", "country": "US", "lat": 40.785091, "lon": -73.968285},
    {"id": "WS002", "name": "Hyde Park", "city": "London", "country": "GB", "lat": 51.507359, "lon": -0.165180},
    {"id": "WS003", "name": "Shinjuku Gyoen", "city": "Tokyo", "country": "JP", "lat": 35.685360, "lon": 139.710041},
    {"id": "WS004", "name": "Tiergarten", "city": "Berlin", "country": "DE", "lat": 52.515810, "lon": 13.369403},
    {"id": "WS005", "name": "Chapultepec", "city": "Mexico City", "country": "MX", "lat": 19.419605, "lon": -99.191604},
]

# Define weather condition possibilities
WEATHER_CONDITIONS = [
    {"condition": "Clear", "description": "Clear sky", "probability": 0.3},
    {"condition": "Clouds", "description": "Few clouds", "probability": 0.3},
    {"condition": "Clouds", "description": "Scattered clouds", "probability": 0.1},
    {"condition": "Clouds", "description": "Broken clouds", "probability": 0.1},
    {"condition": "Rain", "description": "Light rain", "probability": 0.1},
    {"condition": "Rain", "description": "Moderate rain", "probability": 0.05},
    {"condition": "Rain", "description": "Heavy rain", "probability": 0.02},
    {"condition": "Thunderstorm", "description": "Thunderstorm", "probability": 0.02},
    {"condition": "Snow", "description": "Light snow", "probability": 0.01},
    {"condition": "Fog", "description": "Fog", "probability": 0.01},
]

class WeatherSimulator:
    """Class to simulate weather station data."""
    
    def __init__(self, station_id=None):
        """
        Initialize the weather simulator.
        
        Args:
            station_id: Optional specific station ID to simulate
        """
        self.stations = WEATHER_STATIONS
        
        # Filter to specific station if provided
        if station_id:
            self.stations = [s for s in self.stations if s["id"] == station_id]
            if not self.stations:
                logger.error(f"No station found with ID: {station_id}")
                raise ValueError(f"No station found with ID: {station_id}")
        
        # Initialize base conditions for each station
        self.station_conditions = {}
        for station in self.stations:
            self.station_conditions[station["id"]] = self._initialize_conditions(station)
    
    def _initialize_conditions(self, station: Dict[str, Any]) -> Dict[str, Any]:
        """
        Initialize weather conditions for a station.
        
        Args:
            station: Station information
            
        Returns:
            Dict with base weather conditions
        """
        # Base temperature on latitude (colder towards poles)
        base_temp = 25 - abs(station["lat"]) * 0.5
        
        return {
            "temperature": base_temp + random.uniform(-5, 5),
            "humidity": random.uniform(40, 80),
            "pressure": random.uniform(1000, 1020),
            "wind_speed": random.uniform(0, 15),
            "wind_direction": random.uniform(0, 360),
            "clouds": random.uniform(0, 100),
            "weather_condition": self._select_weather_condition()
        }
    
    def _select_weather_condition(self) -> Dict[str, str]:
        """
        Randomly select a weather condition based on probabilities.
        
        Returns:
            Dict with weather condition and description
        """
        r = random.random()
        cumulative = 0
        
        for condition in WEATHER_CONDITIONS:
            cumulative += condition["probability"]
            if r <= cumulative:
                return {
                    "main": condition["condition"],
                    "description": condition["description"]
                }
        
        # Fallback
        return {
            "main": "Clear",
            "description": "Clear sky"
        }
    
    def _update_conditions(self, station_id: str) -> None:
        """
        Update weather conditions for a station with small random changes.
        
        Args:
            station_id: Station ID to update
        """
        conditions = self.station_conditions[station_id]
        
        # Update temperature (small change)
        conditions["temperature"] += random.uniform(-0.5, 0.5)
        
        # Update humidity (small change)
        conditions["humidity"] += random.uniform(-2, 2)
        conditions["humidity"] = max(0, min(100, conditions["humidity"]))
        
        # Update pressure (small change)
        conditions["pressure"] += random.uniform(-1, 1)
        
        # Update wind
        conditions["wind_speed"] += random.uniform(-1, 1)
        conditions["wind_speed"] = max(0, conditions["wind_speed"])
        conditions["wind_direction"] += random.uniform(-10, 10) % 360
        
        # Update clouds
        conditions["clouds"] += random.uniform(-5, 5)
        conditions["clouds"] = max(0, min(100, conditions["clouds"]))
        
        # Occasionally change weather condition (10% chance)
        if random.random() < 0.1:
            conditions["weather_condition"] = self._select_weather_condition()
    
    def generate_reading(self, station_id: str) -> Dict[str, Any]:
        """
        Generate a single weather reading for a station.
        
        Args:
            station_id: Station ID to generate reading for
            
        Returns:
            Dict with the weather reading
        """
        # Find the station
        station = next((s for s in self.stations if s["id"] == station_id), None)
        if not station:
            logger.error(f"No station found with ID: {station_id}")
            raise ValueError(f"No station found with ID: {station_id}")
        
        # Update conditions
        self._update_conditions(station_id)
        conditions = self.station_conditions[station_id]
        
        # Create the reading
        timestamp = datetime.now().isoformat()
        
        reading = {
            "timestamp": timestamp,
            "source": "weather_station",
            "station_id": station["id"],
            "station_name": station["name"],
            "city": station["city"],
            "country": station["country"],
            "coordinates": {
                "lat": station["lat"],
                "lon": station["lon"]
            },
            "temperature": round(conditions["temperature"], 2),
            "humidity": round(conditions["humidity"], 2),
            "pressure": round(conditions["pressure"], 2),
            "wind_speed": round(conditions["wind_speed"], 2),
            "wind_direction": round(conditions["wind_direction"], 2),
            "clouds": round(conditions["clouds"], 2),
            "weather_condition": conditions["weather_condition"]["main"],
            "weather_description": conditions["weather_condition"]["description"]
        }
        
        return reading
    
    def generate_readings(self) -> List[Dict[str, Any]]:
        """
        Generate readings for all stations.
        
        Returns:
            List of weather readings
        """
        readings = []
        
        for station in self.stations:
            reading = self.generate_reading(station["id"])
            readings.append(reading)
        
        return readings
    
    def save_readings(self, readings: List[Dict[str, Any]]) -> None:
        """
        Save readings to files.
        
        Args:
            readings: List of weather readings
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for reading in readings:
            station_id = reading.get("station_id", "unknown")
            filename = f"{RAW_DATA_PATH}/station_{station_id}_{timestamp}.json"
            
            save_to_json(reading, filename)
            logger.info(f"Saved station reading to {filename}")

def simulate_continuous(interval_seconds: int = 300, max_iterations: int = None):
    """
    Run continuous simulation at regular intervals.
    
    Args:
        interval_seconds: Interval between readings in seconds
        max_iterations: Maximum number of iterations (None for unlimited)
    """
    simulator = WeatherSimulator()
    iteration = 0
    
    try:
        while max_iterations is None or iteration < max_iterations:
            logger.info(f"Generating weather station readings (iteration {iteration+1})")
            
            # Generate and save readings
            readings = simulator.generate_readings()
            simulator.save_readings(readings)
            
            # Log summary
            logger.info(f"Generated {len(readings)} station readings")
            
            # Stop if we've reached max iterations
            iteration += 1
            if max_iterations is not None and iteration >= max_iterations:
                break
            
            # Sleep until next interval
            logger.info(f"Sleeping for {interval_seconds} seconds...")
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    except Exception as e:
        logger.error(f"Error in weather simulation: {e}")

def main():
    """Main function to run the weather simulator."""
    parser = argparse.ArgumentParser(description='Weather Station Simulator')
    parser.add_argument('--interval', type=int, default=300, 
                        help='Interval between readings in seconds')
    parser.add_argument('--iterations', type=int, default=None, 
                        help='Maximum number of iterations (default: unlimited)')
    parser.add_argument('--station', type=str, default=None, 
                        help='Generate data for a specific station ID')
    parser.add_argument('--single', action='store_true', 
                        help='Generate a single reading and exit')
    
    args = parser.parse_args()
    
    try:
        if args.single:
            # Generate a single reading
            simulator = WeatherSimulator(station_id=args.station)
            
            if args.station:
                readings = [simulator.generate_reading(args.station)]
            else:
                readings = simulator.generate_readings()
            
            simulator.save_readings(readings)
            logger.info(f"Generated {len(readings)} station readings")
        else:
            # Continuous simulation
            simulate_continuous(args.interval, args.iterations)
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
