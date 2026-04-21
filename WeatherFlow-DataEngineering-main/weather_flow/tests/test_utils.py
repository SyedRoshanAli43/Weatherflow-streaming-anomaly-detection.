"""
Tests for the utility functions in scripts/utils.py.
"""

import os
import sys
import json
import tempfile
import unittest
from unittest import mock

# Add parent directory to path to import from scripts
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import (
    save_to_json, make_api_request, get_timestamp, parse_weather_data
)

class TestUtils(unittest.TestCase):
    """Test case for utility functions."""
    
    def test_save_to_json(self):
        """Test saving data to a JSON file."""
        # Create test data
        test_data = {"key": "value", "number": 42}
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            # Test saving to the file
            result = save_to_json(test_data, tmp_path)
            self.assertTrue(result)
            
            # Verify the file exists and contains the expected data
            with open(tmp_path, 'r') as f:
                saved_data = json.load(f)
            
            self.assertEqual(saved_data, test_data)
        finally:
            # Clean up
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
    
    @mock.patch('scripts.utils.requests.get')
    def test_make_api_request(self, mock_get):
        """Test making API requests."""
        # Mock the requests.get method
        mock_response = mock.Mock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test the function
        url = "https://api.example.com/endpoint"
        params = {"param1": "value1"}
        result = make_api_request(url, params)
        
        # Verify the request was made correctly
        mock_get.assert_called_once_with(url, params=params, timeout=10)
        
        # Verify the result
        self.assertEqual(result, {"data": "test"})
    
    def test_get_timestamp(self):
        """Test getting a timestamp."""
        # Get a timestamp
        timestamp = get_timestamp()
        
        # Verify it's a string
        self.assertIsInstance(timestamp, str)
        
        # Verify it contains date and time elements (basic check)
        self.assertIn('T', timestamp)  # ISO format separator
    
    def test_parse_weather_data_openweather(self):
        """Test parsing OpenWeatherMap data."""
        # Create test data
        test_data = {
            "name": "London",
            "sys": {"country": "GB"},
            "main": {
                "temp": 15.5,
                "humidity": 75,
                "pressure": 1012
            },
            "wind": {
                "speed": 4.5,
                "deg": 270
            },
            "weather": [
                {
                    "main": "Clouds",
                    "description": "scattered clouds"
                }
            ],
            "clouds": {"all": 40},
            "coord": {
                "lat": 51.51,
                "lon": -0.13
            }
        }
        
        # Parse the data
        result = parse_weather_data(test_data, 'openweather')
        
        # Verify the result
        self.assertEqual(result['city'], "London")
        self.assertEqual(result['country'], "GB")
        self.assertEqual(result['temperature'], 15.5)
        self.assertEqual(result['humidity'], 75)
        self.assertEqual(result['pressure'], 1012)
        self.assertEqual(result['wind_speed'], 4.5)
        self.assertEqual(result['wind_direction'], 270)
        self.assertEqual(result['weather_condition'], "Clouds")
        self.assertEqual(result['weather_description'], "scattered clouds")
        self.assertEqual(result['clouds'], 40)
        self.assertEqual(result['coordinates']['lat'], 51.51)
        self.assertEqual(result['coordinates']['lon'], -0.13)
    
    def test_parse_weather_data_weatherapi(self):
        """Test parsing WeatherAPI data."""
        # Create test data
        test_data = {
            "location": {
                "name": "London",
                "country": "UK",
                "lat": 51.52,
                "lon": -0.11
            },
            "current": {
                "temp_c": 16.0,
                "humidity": 80,
                "pressure_mb": 1015,
                "wind_kph": 15,
                "wind_degree": 290,
                "condition": {
                    "text": "Partly cloudy"
                },
                "cloud": 25
            }
        }
        
        # Parse the data
        result = parse_weather_data(test_data, 'weatherapi')
        
        # Verify the result
        self.assertEqual(result['city'], "London")
        self.assertEqual(result['country'], "UK")
        self.assertEqual(result['temperature'], 16.0)
        self.assertEqual(result['humidity'], 80)
        self.assertEqual(result['pressure'], 1015)
        self.assertEqual(result['wind_speed'], 15)
        self.assertEqual(result['wind_direction'], 290)
        self.assertEqual(result['weather_condition'], "Partly cloudy")
        self.assertEqual(result['weather_description'], "Partly cloudy")
        self.assertEqual(result['clouds'], 25)
        self.assertEqual(result['coordinates']['lat'], 51.52)
        self.assertEqual(result['coordinates']['lon'], -0.11)

if __name__ == '__main__':
    unittest.main()
