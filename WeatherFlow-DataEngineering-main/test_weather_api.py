#!/usr/bin/env python3
"""
Test script to verify Weather API functionality
"""

import requests
import json
from datetime import datetime

def fetch_city_weather(city, country, api_key):
    """Fetch weather data for a specific city"""
    try:
        # Fetch weather data from WeatherAPI
        weather_api_url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city},{country}&aqi=no"
        
        print(f"Fetching data for {city}, {country}...")
        response = requests.get(weather_api_url)
        
        if response.status_code != 200:
            print(f"Error: Failed to fetch weather data: {response.text}")
            return None
        
        weather_data = response.json()
        
        # Extract relevant data
        current = weather_data.get('current', {})
        location = weather_data.get('location', {})
        
        # Print formatted results
        print(f"\n{'-'*50}")
        print(f"Weather for {location.get('name')}, {location.get('country')}:")
        print(f"Local Time: {location.get('localtime')}")
        print(f"Temperature: {current.get('temp_c')}°C / {current.get('temp_f')}°F")
        print(f"Condition: {current.get('condition', {}).get('text')}")
        print(f"Humidity: {current.get('humidity')}%")
        print(f"Wind: {current.get('wind_kph')} km/h {current.get('wind_dir')}")
        print(f"Pressure: {current.get('pressure_mb')} mb")
        print(f"Precipitation: {current.get('precip_mm')} mm")
        print(f"Cloud Cover: {current.get('cloud')}%")
        
        # Create formatted data for the WeatherFlow project format
        processed_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "weatherapi",
            "city": location.get('name', city),
            "country": location.get('country', country),
            "coordinates": {
                "lat": location.get('lat', 0),
                "lon": location.get('lon', 0)
            },
            "temperature": current.get('temp_c', 0),
            "humidity": current.get('humidity', 0),
            "pressure": current.get('pressure_mb', 0),
            "wind_speed": current.get('wind_kph', 0) / 3.6,  # Convert to m/s
            "wind_direction": current.get('wind_degree', 0),
            "clouds": current.get('cloud', 0),
            "weather_condition": current.get('condition', {}).get('text', 'Unknown'),
            "weather_description": current.get('condition', {}).get('text', 'Unknown'),
            "processed_timestamp": datetime.now().strftime("%Y-%m-%dT%H-%M-%S"),
            "temperature_fahrenheit": current.get('temp_f', 0),
            "weather_severity": "mild"  # Default value
        }
        
        return processed_data
        
    except Exception as e:
        print(f"Error fetching data for {city}: {e}")
        return None

def main():
    print("Testing WeatherAPI connection...")
    
    # WeatherAPI key via environment variable (do not hardcode secrets)
    import os
    api_key = os.environ.get("WEATHERAPI_KEY")
    if not api_key:
        raise RuntimeError("Set WEATHERAPI_KEY in your environment before running this test.")
    
    # List of cities to test
    cities = [
        {"city": "London", "country": "UK"},
        {"city": "Berlin", "country": "Germany"},
        {"city": "New York", "country": "US"},
        {"city": "Tokyo", "country": "Japan"},
        {"city": "Sydney", "country": "Australia"}
    ]
    
    try:
        all_data = []
        
        # Fetch data for each city
        for city_info in cities:
            city_data = fetch_city_weather(city_info["city"], city_info["country"], api_key)
            if city_data:
                all_data.append(city_data)
        
        print(f"\n{'-'*50}")
        print(f"Successfully fetched weather data for {len(all_data)} cities")
        
        # Show sample of the formatted data
        if all_data:
            print("\nSample of formatted data for WeatherFlow:")
            print(json.dumps(all_data[0], indent=2))
            
            # Save to a sample file
            sample_file = "sample_weather_data.json"
            with open(sample_file, 'w') as f:
                json.dump(all_data, f, indent=2)
            print(f"\nSaved all weather data to {sample_file}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
