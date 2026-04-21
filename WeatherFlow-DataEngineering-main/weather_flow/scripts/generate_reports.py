"""
Generate daily reports from processed weather data.
"""

import os
import json
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import glob
from typing import Dict, Any, List

# Define paths directly
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'reports')

# Create directories if they don't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(REPORTS_PATH, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_weather_data(days=1):
    """
    Load weather data from the last N days.
    
    Args:
        days: Number of days of data to load
        
    Returns:
        pd.DataFrame: DataFrame with weather data
    """
    # Calculate the date threshold
    threshold_date = datetime.now() - timedelta(days=days)
    
    # Find all JSON files in the raw data directory
    json_files = glob.glob(f"{RAW_DATA_PATH}/*.json")
    
    logger.info(f"Found {len(json_files)} raw data files")
    
    # Initialize an empty list to store data
    all_data = []
    
    # Process each file
    for file_path in json_files:
        # Get the file modification time
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        
        # Skip files older than the threshold
        if file_mtime < threshold_date:
            continue
        
        try:
            # Load the data from the file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Process the data to add derived fields
            if 'temperature' in data:
                # Convert to Fahrenheit
                data['temperature_fahrenheit'] = data['temperature'] * 9/5 + 32
                
                # Calculate heat index if humidity is available
                if 'humidity' in data and data['temperature'] * 9/5 + 32 > 80:
                    temp_f = data['temperature'] * 9/5 + 32
                    humidity = data['humidity']
                    
                    # Heat index formula
                    heat_index = -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity
                    heat_index += -0.22475541 * temp_f * humidity - 6.83783e-3 * temp_f**2
                    heat_index += -5.481717e-2 * humidity**2 + 1.22874e-3 * temp_f**2 * humidity
                    heat_index += 8.5282e-4 * temp_f * humidity**2 - 1.99e-6 * temp_f**2 * humidity**2
                    
                    data['heat_index_fahrenheit'] = heat_index
                    data['heat_index_celsius'] = (heat_index - 32) * 5/9
            
            # Add weather severity
            if 'weather_condition' in data:
                condition = data['weather_condition'].lower()
                
                # Define severity levels
                severe_conditions = ['thunderstorm', 'tornado', 'hurricane', 'blizzard']
                moderate_conditions = ['rain', 'snow', 'drizzle', 'fog', 'mist', 'shower']
                mild_conditions = ['cloudy', 'overcast', 'partly cloudy']
                
                if any(cond in condition for cond in severe_conditions):
                    data['weather_severity'] = 'severe'
                elif any(cond in condition for cond in moderate_conditions):
                    data['weather_severity'] = 'moderate'
                elif any(cond in condition for cond in mild_conditions):
                    data['weather_severity'] = 'mild'
                else:
                    data['weather_severity'] = 'normal'
            
            # Add the processed data to our list
            all_data.append(data)
            
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
    
    # Convert to DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        logger.info(f"Loaded and processed {len(df)} weather records")
        return df
    else:
        logger.warning("No data found for the specified time period")
        return pd.DataFrame()

def generate_temperature_report(df, output_dir):
    """
    Generate a temperature report with average temperatures by city.
    
    Args:
        df: DataFrame with weather data
        output_dir: Directory to save the report
    """
    if df.empty:
        logger.warning("Empty DataFrame provided. Cannot generate temperature report.")
        return
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Timestamp for the report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Group by city and calculate average temperature
        temp_by_city = df.groupby('city')['temperature'].agg(['mean', 'min', 'max']).reset_index()
        temp_by_city.columns = ['City', 'Avg Temp (°C)', 'Min Temp (°C)', 'Max Temp (°C)']
        
        # Save to CSV
        csv_path = f"{output_dir}/temperature_report_{timestamp}.csv"
        temp_by_city.to_csv(csv_path, index=False)
        logger.info(f"Temperature report saved to {csv_path}")
        
        # Create a bar chart
        plt.figure(figsize=(12, 8))
        plt.bar(temp_by_city['City'], temp_by_city['Avg Temp (°C)'], color='skyblue')
        plt.xlabel('City')
        plt.ylabel('Average Temperature (°C)')
        plt.title('Average Temperature by City')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save the chart
        chart_path = f"{output_dir}/temperature_chart_{timestamp}.png"
        plt.savefig(chart_path)
        plt.close()
        logger.info(f"Temperature chart saved to {chart_path}")
        
    except Exception as e:
        logger.error(f"Error generating temperature report: {e}")

def generate_weather_condition_report(df, output_dir):
    """
    Generate a report on weather conditions by city.
    
    Args:
        df: DataFrame with weather data
        output_dir: Directory to save the report
    """
    if df.empty:
        logger.warning("Empty DataFrame provided. Cannot generate weather condition report.")
        return
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Timestamp for the report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Count occurrences of each weather condition by city
        condition_counts = df.groupby(['city', 'weather_condition']).size().reset_index(name='count')
        
        # Pivot the table for better readability
        pivot_table = condition_counts.pivot_table(
            index='city', 
            columns='weather_condition', 
            values='count', 
            fill_value=0
        ).reset_index()
        
        # Save to CSV
        csv_path = f"{output_dir}/weather_condition_report_{timestamp}.csv"
        pivot_table.to_csv(csv_path, index=False)
        logger.info(f"Weather condition report saved to {csv_path}")
        
        # Create a pie chart for each city
        for city in df['city'].unique():
            city_data = condition_counts[condition_counts['city'] == city]
            
            plt.figure(figsize=(10, 8))
            plt.pie(city_data['count'], labels=city_data['weather_condition'], autopct='%1.1f%%')
            plt.title(f'Weather Conditions in {city}')
            plt.tight_layout()
            
            # Save the chart
            chart_path = f"{output_dir}/weather_condition_chart_{city.lower().replace(' ', '_')}_{timestamp}.png"
            plt.savefig(chart_path)
            plt.close()
            logger.info(f"Weather condition chart for {city} saved to {chart_path}")
        
    except Exception as e:
        logger.error(f"Error generating weather condition report: {e}")

def generate_severity_report(df, output_dir):
    """
    Generate a report on weather severity by city.
    
    Args:
        df: DataFrame with weather data
        output_dir: Directory to save the report
    """
    if df.empty or 'weather_severity' not in df.columns:
        logger.warning("Cannot generate severity report. DataFrame is empty or missing 'weather_severity' column.")
        return
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Timestamp for the report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Count occurrences of each severity level by city
        severity_counts = df.groupby(['city', 'weather_severity']).size().reset_index(name='count')
        
        # Pivot the table for better readability
        pivot_table = severity_counts.pivot_table(
            index='city', 
            columns='weather_severity', 
            values='count', 
            fill_value=0
        ).reset_index()
        
        # Save to CSV
        csv_path = f"{output_dir}/weather_severity_report_{timestamp}.csv"
        pivot_table.to_csv(csv_path, index=False)
        logger.info(f"Weather severity report saved to {csv_path}")
        
        # Create a stacked bar chart
        severity_order = ['normal', 'mild', 'moderate', 'severe']
        pivot_data = pivot_table.set_index('city')
        
        # Make sure all columns exist
        for severity in severity_order:
            if severity not in pivot_data.columns:
                pivot_data[severity] = 0
        
        pivot_data = pivot_data[severity_order]
        
        plt.figure(figsize=(12, 8))
        pivot_data.plot(kind='bar', stacked=True, figsize=(12, 8), 
                        color=['green', 'blue', 'orange', 'red'])
        plt.xlabel('City')
        plt.ylabel('Count')
        plt.title('Weather Severity by City')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save the chart
        chart_path = f"{output_dir}/weather_severity_chart_{timestamp}.png"
        plt.savefig(chart_path)
        plt.close()
        logger.info(f"Weather severity chart saved to {chart_path}")
        
    except Exception as e:
        logger.error(f"Error generating severity report: {e}")

def generate_daily_summary_report(df, output_dir):
    """
    Generate a comprehensive daily summary report.
    
    Args:
        df: DataFrame with weather data
        output_dir: Directory to save the report
    """
    if df.empty:
        logger.warning("Empty DataFrame provided. Cannot generate daily summary report.")
        return
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Timestamp for the report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_date = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Create a HTML report
        html_path = f"{output_dir}/daily_weather_summary_{timestamp}.html"
        
        # Generate the HTML content
        html_content = f"""
        <html>
        <head>
            <title>Daily Weather Summary - {report_date}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2 {{ color: #2c3e50; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .severe {{ color: red; font-weight: bold; }}
                .moderate {{ color: orange; }}
                .mild {{ color: blue; }}
                .normal {{ color: green; }}
            </style>
        </head>
        <body>
            <h1>Daily Weather Summary - {report_date}</h1>
        """
        
        # Add temperature section
        html_content += """
            <h2>Temperature Summary</h2>
            <table>
                <tr>
                    <th>City</th>
                    <th>Avg Temp (°C)</th>
                    <th>Min Temp (°C)</th>
                    <th>Max Temp (°C)</th>
                    <th>Avg Humidity (%)</th>
                </tr>
        """
        
        # Group by city and calculate metrics
        temp_summary = df.groupby('city').agg({
            'temperature': ['mean', 'min', 'max'],
            'humidity': 'mean'
        }).reset_index()
        
        # Flatten the column names
        temp_summary.columns = ['City', 'Avg Temp (°C)', 'Min Temp (°C)', 'Max Temp (°C)', 'Avg Humidity (%)']
        
        # Add rows to the temperature table
        for _, row in temp_summary.iterrows():
            html_content += f"""
                <tr>
                    <td>{row['City']}</td>
                    <td>{row['Avg Temp (°C)']:.1f}</td>
                    <td>{row['Min Temp (°C)']:.1f}</td>
                    <td>{row['Max Temp (°C)']:.1f}</td>
                    <td>{row['Avg Humidity (%)']:.1f}</td>
                </tr>
            """
        
        html_content += """
            </table>
        """
        
        # Add weather conditions section
        html_content += """
            <h2>Weather Conditions</h2>
            <table>
                <tr>
                    <th>City</th>
                    <th>Most Common Condition</th>
                    <th>Severity</th>
                </tr>
        """
        
        # Find the most common weather condition for each city
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            most_common_condition = city_data['weather_condition'].mode()[0]
            severity = city_data[city_data['weather_condition'] == most_common_condition]['weather_severity'].mode()[0]
            
            html_content += f"""
                <tr>
                    <td>{city}</td>
                    <td>{most_common_condition}</td>
                    <td class="{severity}">{severity.capitalize()}</td>
                </tr>
            """
        
        html_content += """
            </table>
        """
        
        # Add extreme weather alerts
        severe_weather = df[df['weather_severity'] == 'severe']
        if not severe_weather.empty:
            html_content += """
                <h2>Extreme Weather Alerts</h2>
                <table>
                    <tr>
                        <th>City</th>
                        <th>Condition</th>
                        <th>Temperature (°C)</th>
                        <th>Wind Speed</th>
                    </tr>
            """
            
            for _, row in severe_weather.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['city']}</td>
                        <td>{row['weather_condition']}</td>
                        <td>{row['temperature']:.1f}</td>
                        <td>{row['wind_speed']:.1f}</td>
                    </tr>
                """
            
            html_content += """
                </table>
            """
        
        # Close the HTML document
        html_content += """
        </body>
        </html>
        """
        
        # Write the HTML file
        with open(html_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Daily summary report saved to {html_path}")
        
    except Exception as e:
        logger.error(f"Error generating daily summary report: {e}")

def main():
    """Generate all reports."""
    try:
        logger.info("Starting report generation")
        
        # Load data from the last day
        df = load_weather_data(days=1)
        
        if df.empty:
            logger.warning("No data available for report generation")
            return
        
        # Generate reports
        generate_temperature_report(df, REPORTS_PATH)
        generate_weather_condition_report(df, REPORTS_PATH)
        generate_severity_report(df, REPORTS_PATH)
        if 'generate_daily_summary_report' in globals():
            generate_daily_summary_report(df, REPORTS_PATH)
        
        logger.info("Report generation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in report generation: {e}")

if __name__ == "__main__":
    main()
