"""
WeatherFlow Flask Dashboard
A modern, aesthetically pleasing dashboard for visualizing weather data.
"""

import os
import json
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from flask import Flask, render_template, jsonify, Response, request

# Define paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'reports')

# Create directories if they don't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(REPORTS_PATH, exist_ok=True)

# Custom JSON encoder to handle NaN values
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
            return None
        return super().default(obj)

# Create Flask app
app = Flask(__name__, 
            static_folder=os.path.join(PROJECT_ROOT, 'dashboard', 'static'),
            template_folder=os.path.join(PROJECT_ROOT, 'dashboard', 'templates'))

# Use custom JSON encoder
app.json_encoder = CustomJSONEncoder

def load_weather_data():
    """Load and process weather data from JSON files."""
    all_data = []
    
    # Check if the data directory exists
    if not os.path.exists(PROCESSED_DATA_PATH):
        print(f"Processed data directory does not exist: {PROCESSED_DATA_PATH}")
        # Create a sample data point for testing if no data exists
        sample_data = {
            "timestamp": "2025-05-24T03:30:00",
            "source": "sample",
            "city": "Berlin",
            "country": "Germany",
            "coordinates": {"lat": 52.5167, "lon": 13.4},
            "temperature": 15.0,
            "humidity": 45,
            "pressure": 1018.0,
            "wind_speed": 2.5,
            "wind_direction": 245,
            "clouds": 10,
            "weather_condition": "Clear",
            "weather_description": "Clear sky",
            "processed_timestamp": "2025-05-24T03:30:00",
            "temperature_fahrenheit": 59.0,
            "weather_severity": "mild"
        }
        all_data.append(sample_data)
        return pd.DataFrame([sample_data])
    
    try:
        # Get all processed weather data files
        json_files = glob.glob(os.path.join(PROCESSED_DATA_PATH, "processed_*.json"))
        
        if not json_files:
            print(f"No weather data files found in {PROCESSED_DATA_PATH}")
            # Create a sample data point for testing if no files exist
            sample_data = {
                "timestamp": "2025-05-24T03:30:00",
                "source": "sample",
                "city": "Berlin",
                "country": "Germany",
                "coordinates": {"lat": 52.5167, "lon": 13.4},
                "temperature": 15.0,
                "humidity": 45,
                "pressure": 1018.0,
                "wind_speed": 2.5,
                "wind_direction": 245,
                "clouds": 10,
                "weather_condition": "Clear",
                "weather_description": "Clear sky",
                "processed_timestamp": "2025-05-24T03:30:00",
                "temperature_fahrenheit": 59.0,
                "weather_severity": "mild"
            }
            all_data.append(sample_data)
        else:
            for file_path in json_files:
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        # Ensure all required fields are present
                        required_fields = ['city', 'temperature', 'humidity', 'wind_speed']
                        if all(field in data for field in required_fields):
                            all_data.append(data)
                        else:
                            print(f"File {file_path} missing required fields")
                except json.JSONDecodeError as e:
                    print(f"JSON decode error in file {file_path}: {e}")
                    continue
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                    continue
    except Exception as e:
        print(f"Error accessing data directory: {e}")
    
    if not all_data:
        print("No valid data found, creating sample data")
        # Create a sample data point if no valid data was found
        sample_data = {
            "timestamp": "2025-05-24T03:30:00",
            "source": "sample",
            "city": "Berlin",
            "country": "Germany",
            "coordinates": {"lat": 52.5167, "lon": 13.4},
            "temperature": 15.0,
            "humidity": 45,
            "pressure": 1018.0,
            "wind_speed": 2.5,
            "wind_direction": 245,
            "clouds": 10,
            "weather_condition": "Clear",
            "weather_description": "Clear sky",
            "processed_timestamp": "2025-05-24T03:30:00",
            "temperature_fahrenheit": 59.0,
            "weather_severity": "mild"
        }
        all_data.append(sample_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Convert timestamp strings to datetime objects
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            # Drop rows with invalid timestamps
            df = df.dropna(subset=['timestamp'])
        except Exception as e:
            print(f"Error converting timestamps: {e}")
    
    # Sort by timestamp
    if 'timestamp' in df.columns and not df.empty:
        df = df.sort_values('timestamp')
    
    return df

def weather_icon(condition):
    """Return an emoji based on weather condition."""
    condition = condition.lower() if condition else ""
    if "clear" in condition or "sunny" in condition:
        return "☀️"
    elif "cloud" in condition or "overcast" in condition:
        return "☁️"
    elif "rain" in condition or "drizzle" in condition:
        return "🌧️"
    elif "snow" in condition:
        return "❄️"
    elif "storm" in condition or "thunder" in condition:
        return "⛈️"
    elif "fog" in condition or "mist" in condition:
        return "🌫️"
    else:
        return "🌤️"

def temperature_color(temp):
    """Return a color based on temperature."""
    if temp < 0:
        return "#9ec5fe"  # Very cold (light blue)
    elif temp < 10:
        return "#6ea8fe"  # Cold (blue)
    elif temp < 20:
        return "#a3cfbb"  # Cool (light green)
    elif temp < 30:
        return "#ffda6a"  # Warm (yellow)
    else:
        return "#ea868f"  # Hot (red)

@app.route('/')
def index():
    """Main dashboard page."""
    df = load_weather_data()
    
    if df.empty:
        return render_template('dashboard.html', 
                               error_message="No weather data available. Please check the data directory.",
                               cities=[],
                               weather_data={})
    
    # Get unique cities
    cities = sorted(df['city'].unique())
    
    # Prepare data for each city
    weather_data = {}
    for city in cities:
        city_df = df[df['city'] == city]
        if not city_df.empty:
            # Get the latest data for the city
            latest_row = city_df.iloc[-1]
            latest_data = {}
            
            # Process each column with proper type handling
            for col in city_df.columns:
                value = latest_row[col]
                # Handle different data types
                if isinstance(value, pd.Timestamp):
                    latest_data[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, dict):
                    # Convert dict to a serializable format
                    latest_data[col] = {str(k): str(v) if not isinstance(v, (int, float, bool)) else v 
                                        for k, v in value.items()}
                elif isinstance(value, (int, float, bool, str)) or value is None:
                    latest_data[col] = value
                else:
                    # Convert any other types to string
                    latest_data[col] = str(value)
            
            # Add weather icon and temperature color
            latest_data['weather_icon'] = weather_icon(latest_data.get('weather_condition', ''))
            latest_data['temp_color'] = temperature_color(latest_data.get('temperature', 0))
            
            weather_data[city] = latest_data
    
    return render_template('dashboard.html', 
                           cities=cities,
                           weather_data=weather_data)

@app.route('/api/weather_data')
def api_weather_data():
    """API endpoint to get all weather data."""
    try:
        print("Loading weather data for API endpoint")
        df = load_weather_data()
        
        if df.empty:
            print("No data available in DataFrame")
            return jsonify({'error': 'No data available'})
        
        # Replace NaN values with None for proper JSON serialization
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        
        # Convert DataFrame to list of dicts for JSON serialization
        data = []
        for _, row in df.iterrows():
            try:
                row_dict = {}
                for col in df.columns:
                    try:
                        value = row[col]
                        # Handle different data types for JSON serialization
                        if isinstance(value, pd.Timestamp):
                            row_dict[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                        elif isinstance(value, dict):
                            # Convert dict to a serializable format and handle NaN in dict values
                            processed_dict = {}
                            for k, v in value.items():
                                if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                                    processed_dict[str(k)] = None
                                elif isinstance(v, (int, float, bool)):
                                    processed_dict[str(k)] = v
                                else:
                                    processed_dict[str(k)] = str(v)
                            row_dict[col] = processed_dict
                        elif isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                            row_dict[col] = None
                        elif isinstance(value, (int, float, bool, str)) or value is None:
                            row_dict[col] = value
                        else:
                            # Convert any other types to string
                            row_dict[col] = str(value)
                    except Exception as e:
                        print(f"Error processing column {col}: {e}")
                        row_dict[col] = None
                data.append(row_dict)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        
        print(f"Returning {len(data)} data points")
        # Use custom JSON serialization to handle NaN values
        return Response(
            json.dumps(data, cls=CustomJSONEncoder),
            mimetype='application/json'
        )
    except Exception as e:
        print(f"API error: {e}")
        return jsonify({'error': f'API error: {str(e)}'})


@app.route('/api/city/<city>')
def api_city_data(city):
    """API endpoint to get weather data for a specific city."""
    try:
        print(f"Loading weather data for city: {city}")
        df = load_weather_data()
        
        if df.empty:
            print("No data available in DataFrame")
            return jsonify({'error': 'No data available'})
        
        city_df = df[df['city'] == city]
        
        if city_df.empty:
            print(f"No data available for city: {city}")
            return jsonify({'error': f'No data available for {city}'})
        
        # Replace NaN values with None for proper JSON serialization
        city_df = city_df.replace({np.nan: None, np.inf: None, -np.inf: None})
        
        # Convert DataFrame to list of dicts for JSON serialization
        data = []
        for _, row in city_df.iterrows():
            try:
                row_dict = {}
                for col in city_df.columns:
                    try:
                        value = row[col]
                        # Handle different data types for JSON serialization
                        if isinstance(value, pd.Timestamp):
                            row_dict[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                        elif isinstance(value, dict):
                            # Convert dict to a serializable format and handle NaN in dict values
                            processed_dict = {}
                            for k, v in value.items():
                                if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                                    processed_dict[str(k)] = None
                                elif isinstance(v, (int, float, bool)):
                                    processed_dict[str(k)] = v
                                else:
                                    processed_dict[str(k)] = str(v)
                            row_dict[col] = processed_dict
                        elif isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                            row_dict[col] = None
                        elif isinstance(value, (int, float, bool, str)) or value is None:
                            row_dict[col] = value
                        else:
                            # Convert any other types to string
                            row_dict[col] = str(value)
                    except Exception as e:
                        print(f"Error processing column {col}: {e}")
                        row_dict[col] = None
                data.append(row_dict)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        
        print(f"Returning {len(data)} data points for city: {city}")
        # Use custom JSON serialization to handle NaN values
        return Response(
            json.dumps(data, cls=CustomJSONEncoder),
            mimetype='application/json'
        )
    except Exception as e:
        print(f"API error for city {city}: {e}")
        return jsonify({'error': f'API error: {str(e)}'})


# WeatherAPI key via environment variable (never hardcode secrets)
WEATHER_API_KEY = os.environ.get("WEATHERAPI_KEY", "")

@app.route('/api/add_city', methods=['POST'])
def add_city():
    """API endpoint to add a new city and fetch its weather data."""
    try:
        # Get city and country from request
        data = request.get_json()
        city = data.get('city')
        country = data.get('country')
        
        if not city or not country:
            return jsonify({'error': 'City and country are required'})
        
        print(f"Adding new city: {city}, {country}")
        
        if not WEATHER_API_KEY:
            return jsonify({'error': 'Missing WEATHERAPI_KEY environment variable on server'})

        # Fetch weather data from WeatherAPI
        response = requests.get(
            "http://api.weatherapi.com/v1/current.json",
            params={"key": WEATHER_API_KEY, "q": f"{city},{country}", "aqi": "no"},
            timeout=10,
        )
        if response.status_code != 200:
            return jsonify({'error': f"Failed to fetch weather data: {response.text}"})
        
        weather_data = response.json()
        
        # Extract relevant data
        current = weather_data.get('current', {})
        location = weather_data.get('location', {})
        
        # Create processed data format
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
        
        # Save to a file
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        filename = f"processed_weatherapi_{city.lower()}_{timestamp}.json"
        filepath = os.path.join(PROCESSED_DATA_PATH, filename)
        
        with open(filepath, 'w') as f:
            json.dump(processed_data, f, indent=4)
        
        return jsonify({
            'message': f"Successfully added {city}, {country}",
            'data': processed_data
        })
    
    except Exception as e:
        print(f"Error adding city: {e}")
        return jsonify({'error': f"Error adding city: {str(e)}"})

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs(os.path.join(PROJECT_ROOT, 'dashboard', 'templates'), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, 'dashboard', 'static'), exist_ok=True)
    
    # Create data directories if they don't exist
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    os.makedirs(REPORTS_PATH, exist_ok=True)
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=8501)
