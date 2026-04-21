"""
Tests for the weather simulator in scripts/weather_simulator.py.
"""

import os
import sys
import unittest
from unittest import mock

# Add parent directory to path to import from scripts
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.weather_simulator import WeatherSimulator, WEATHER_STATIONS

class TestWeatherSimulator(unittest.TestCase):
    """Test case for the weather simulator."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create a simulator instance
        self.simulator = WeatherSimulator()
    
    def test_init(self):
        """Test initialization of the simulator."""
        # Verify the simulator has the correct stations
        self.assertEqual(len(self.simulator.stations), len(WEATHER_STATIONS))
        
        # Verify station conditions were initialized
        self.assertEqual(len(self.simulator.station_conditions), len(WEATHER_STATIONS))
        
        # Test initialization with specific station
        specific_simulator = WeatherSimulator(station_id="WS001")
        self.assertEqual(len(specific_simulator.stations), 1)
        self.assertEqual(specific_simulator.stations[0]["id"], "WS001")
    
    def test_init_invalid_station(self):
        """Test initialization with an invalid station ID."""
        with self.assertRaises(ValueError):
            WeatherSimulator(station_id="INVALID")
    
    def test_generate_reading(self):
        """Test generating a single reading."""
        # Generate a reading for a specific station
        reading = self.simulator.generate_reading("WS001")
        
        # Verify the reading has the expected fields
        self.assertEqual(reading["station_id"], "WS001")
        self.assertEqual(reading["source"], "weather_station")
        
        # Check data types and ranges
        self.assertIsInstance(reading["temperature"], float)
        self.assertIsInstance(reading["humidity"], float)
        self.assertIsInstance(reading["pressure"], float)
        self.assertIsInstance(reading["wind_speed"], float)
        self.assertIsInstance(reading["wind_direction"], float)
        
        # Check values are in reasonable ranges
        self.assertGreater(reading["temperature"], -50)
        self.assertLess(reading["temperature"], 50)
        
        self.assertGreaterEqual(reading["humidity"], 0)
        self.assertLessEqual(reading["humidity"], 100)
        
        self.assertGreater(reading["pressure"], 900)
        self.assertLess(reading["pressure"], 1100)
        
        self.assertGreaterEqual(reading["wind_speed"], 0)
        
        self.assertGreaterEqual(reading["wind_direction"], 0)
        self.assertLess(reading["wind_direction"], 360)
    
    def test_generate_readings(self):
        """Test generating readings for all stations."""
        # Generate readings
        readings = self.simulator.generate_readings()
        
        # Verify we got the expected number of readings
        self.assertEqual(len(readings), len(WEATHER_STATIONS))
        
        # Verify each reading has a unique station ID
        station_ids = [reading["station_id"] for reading in readings]
        self.assertEqual(len(station_ids), len(set(station_ids)))
    
    @mock.patch('scripts.weather_simulator.save_to_json')
    def test_save_readings(self, mock_save):
        """Test saving readings to files."""
        # Generate readings
        readings = self.simulator.generate_readings()
        
        # Save the readings
        self.simulator.save_readings(readings)
        
        # Verify save_to_json was called for each reading
        self.assertEqual(mock_save.call_count, len(readings))

if __name__ == '__main__':
    unittest.main()
