"""
Dashboard application for WeatherFlow.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta

# Import the data access layer
from dashboard.data_access import get_weather_data_access

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from flask import Flask

# Suppress React defaultProps warnings
os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning'

# This is needed to suppress React warnings in the browser console
import warnings
warnings.filterwarnings('ignore')

# Set environment variable to disable React dev warnings
os.environ['REACT_APP_SILENCE_WARNINGS'] = '1'

# Define paths directly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')
REPORTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'reports')

# Create directories if they don't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(REPORTS_PATH, exist_ok=True)

# Dashboard configuration
DASHBOARD_PORT = 8051

# Initialize the Flask server and Dash app
server = Flask(__name__)
app = dash.Dash(
    __name__, 
    server=server, 
    suppress_callback_exceptions=True,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ],
    # Use our custom HTML template with warning suppression
    index_string=open('dashboard/templates/index.html').read()
)

# Custom CSS for a more professional look
external_stylesheets = [
    'https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap'
]

for stylesheet in external_stylesheets:
    app.css.append_css({"external_url": stylesheet})

# Define color schemes
COLORS = {
    'background': '#f8f9fa',
    'text': '#343a40',
    'primary': '#0d6efd',
    'secondary': '#6c757d',
    'success': '#198754',
    'danger': '#dc3545',
    'warning': '#ffc107',
    'info': '#0dcaf0',
    'light': '#f8f9fa',
    'dark': '#212529',
    'white': '#ffffff',
    'card': '#ffffff',
    'border': '#dee2e6'
}

# Define the app layout with improved aesthetics
app.layout = html.Div([
    # Header with logo and title
    html.Div([
        html.Div([
            html.Img(src='https://cdn-icons-png.flaticon.com/512/1779/1779940.png', 
                    style={'height': '50px', 'margin-right': '15px'}),
            html.H1("WeatherFlow Dashboard", 
                   style={'display': 'inline', 'vertical-align': 'middle', 'color': COLORS['white']})
        ], style={'display': 'flex', 'align-items': 'center'}),
        html.P("Real-time weather monitoring and analysis", 
              style={'color': COLORS['light'], 'margin-top': '5px'})
    ], style={
        'background': 'linear-gradient(90deg, #1e3c72 0%, #2a5298 100%)',
        'padding': '20px 30px',
        'border-radius': '10px',
        'margin-bottom': '25px',
        'box-shadow': '0 4px 6px rgba(0, 0, 0, 0.1)'
    }),
    
    # Control panel in a card
    html.Div([
        html.Div([
            html.H3("Control Panel", 
                   style={'margin-bottom': '20px', 'color': COLORS['primary'], 'border-bottom': f'2px solid {COLORS["border"]}', 'padding-bottom': '10px'}),
            
            # Date range and city selection in a grid
            html.Div([
                html.Div([
                    html.Label("Select Date Range:", style={'font-weight': '500', 'margin-bottom': '8px', 'display': 'block'}),
                    dcc.DatePickerRange(
                        id='date-range',
                        min_date_allowed=datetime.now() - timedelta(days=30),
                        max_date_allowed=datetime.now(),
                        start_date=datetime.now() - timedelta(days=7),
                        end_date=datetime.now(),
                        display_format='YYYY-MM-DD',
                        style={'border-radius': '5px'}
                    ),
                ], style={'width': '48%', 'display': 'inline-block', 'margin-right': '2%'}),
                
                html.Div([
                    html.Label("Select Cities:", style={'font-weight': '500', 'margin-bottom': '8px', 'display': 'block'}),
                    dcc.Dropdown(
                        id='city-dropdown',
                        multi=True,
                        placeholder="Select cities...",
                        style={'border-radius': '5px'}
                    ),
                ], style={'width': '48%', 'display': 'inline-block'}),
            ], style={'margin-bottom': '20px'}),
            
            # Weather metric selection
            html.Div([
                html.Label("Select Weather Metric:", style={'font-weight': '500', 'margin-bottom': '8px', 'display': 'block'}),
                dcc.Dropdown(
                    id='metric-dropdown',
                    options=[
                        {'label': 'Temperature (°C)', 'value': 'temperature'},
                        {'label': 'Humidity (%)', 'value': 'humidity'},
                        {'label': 'Pressure (hPa)', 'value': 'pressure'},
                        {'label': 'Wind Speed (m/s)', 'value': 'wind_speed'}
                    ],
                    value='temperature',
                    clearable=False,
                    style={'border-radius': '5px'}
                ),
            ], style={'margin-bottom': '20px'}),
            
            # Update button with improved styling
            html.Button(
                'Update Dashboard', 
                id='update-button', 
                n_clicks=0, 
                style={
                    'background-color': COLORS['primary'],
                    'color': 'white',
                    'border': 'none',
                    'padding': '10px 20px',
                    'border-radius': '5px',
                    'cursor': 'pointer',
                    'font-weight': '500',
                    'transition': 'background-color 0.3s',
                    'box-shadow': '0 2px 4px rgba(0, 0, 0, 0.1)'
                }
            ),
        ], style={
            'background-color': COLORS['card'],
            'padding': '25px',
            'border-radius': '10px',
            'box-shadow': '0 2px 8px rgba(0, 0, 0, 0.1)',
            'margin-bottom': '25px'
        }),
    ]),
    
    # Main chart in a card
    html.Div([
        html.H3("Weather Trends", 
               style={'margin-bottom': '15px', 'color': COLORS['primary'], 'border-bottom': f'2px solid {COLORS["border"]}', 'padding-bottom': '10px'}),
        dcc.Graph(
            id='main-chart',
            config={'displayModeBar': True, 'scrollZoom': True},
            style={'height': '400px'}
        )
    ], style={
        'background-color': COLORS['card'],
        'padding': '25px',
        'border-radius': '10px',
        'box-shadow': '0 2px 8px rgba(0, 0, 0, 0.1)',
        'margin-bottom': '25px'
    }),
    
    # Temperature map and severity chart in cards side by side
    html.Div([
        html.Div([
            html.H3("Temperature Map", 
                   style={'margin-bottom': '15px', 'color': COLORS['primary'], 'border-bottom': f'2px solid {COLORS["border"]}', 'padding-bottom': '10px'}),
            dcc.Graph(
                id='temperature-map',
                config={'displayModeBar': True},
                style={'height': '400px'}
            )
        ], style={
            'width': '49%', 
            'display': 'inline-block',
            'background-color': COLORS['card'],
            'padding': '25px',
            'border-radius': '10px',
            'box-shadow': '0 2px 8px rgba(0, 0, 0, 0.1)',
            'margin-right': '2%'
        }),
        
        html.Div([
            html.H3("Weather Severity", 
                   style={'margin-bottom': '15px', 'color': COLORS['primary'], 'border-bottom': f'2px solid {COLORS["border"]}', 'padding-bottom': '10px'}),
            dcc.Graph(
                id='severity-chart',
                config={'displayModeBar': True},
                style={'height': '400px'}
            )
        ], style={
            'width': '49%', 
            'display': 'inline-block',
            'background-color': COLORS['card'],
            'padding': '25px',
            'border-radius': '10px',
            'box-shadow': '0 2px 8px rgba(0, 0, 0, 0.1)'
        }),
    ], style={'margin-bottom': '25px'}),
    
    # Weather alerts in a card
    html.Div([
        html.H3("Weather Alerts", 
               style={'margin-bottom': '15px', 'color': COLORS['danger'], 'border-bottom': f'2px solid {COLORS["border"]}', 'padding-bottom': '10px'}),
        html.Div(id='alerts-table')
    ], style={
        'background-color': COLORS['card'],
        'padding': '25px',
        'border-radius': '10px',
        'box-shadow': '0 2px 8px rgba(0, 0, 0, 0.1)',
        'margin-bottom': '25px'
    }),
    
    # Footer
    html.Footer([
        html.P([
            " 2025 WeatherFlow | ",
            html.A("Data Sources", href="#", style={'color': COLORS['primary']}),
            " | ",
            html.A("Documentation", href="#", style={'color': COLORS['primary']}),
            " | Last updated: ",
            datetime.now().strftime("%Y-%m-%d %H:%M")
        ], style={'text-align': 'center', 'color': COLORS['secondary']})
    ], style={
        'padding': '15px',
        'border-top': f'1px solid {COLORS["border"]}',
        'margin-top': '10px'
    }),
    
    # Hidden div for storing data
    html.Div(id='data-store', style={'display': 'none'}),
    
    # Auto-refresh interval
    dcc.Interval(
        id='interval-component',
        interval=300*1000,  # refresh every 5 minutes (300 seconds)
        n_intervals=0
    )
], style={
    'font-family': '"Poppins", sans-serif',
    'background-color': COLORS['background'],
    'color': COLORS['text'],
    'padding': '20px',
    'max-width': '1200px',
    'margin': '0 auto'
})

def load_weather_data():
    """
    Load weather data using the data access layer to avoid resource deadlocks.
    
    Returns:
        pd.DataFrame: Combined DataFrame with weather data
    """
    # Get the data access layer instance
    data_access = get_weather_data_access()
    
    # Use the data access layer to load weather data
    # This handles all file access, retries, and error handling
    df = data_access.get_weather_data()
    
    if not df.empty:
        print(f"Successfully loaded data for {len(df)} weather records")
    else:
        print("No weather data found or could be loaded")
        
    return df

@app.callback(
    [Output('city-dropdown', 'options'),
     Output('city-dropdown', 'value')],
    [Input('update-button', 'n_clicks')]
)
def update_city_dropdown(n):
    """Update the city dropdown options using the data access layer."""
    # Get the data access layer
    data_access = get_weather_data_access()
    
    # Get the list of cities
    cities = data_access.get_cities()
    
    if not cities:
        return [], []
    
    options = [{'label': city, 'value': city} for city in cities]
    
    return options, cities

@app.callback(
    Output('main-chart', 'figure'),
    [Input('update-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [dash.dependencies.State('city-dropdown', 'value'),
     dash.dependencies.State('date-range', 'start_date'),
     dash.dependencies.State('date-range', 'end_date'),
     dash.dependencies.State('metric-dropdown', 'value')]
)
def update_main_chart(n_clicks, n_intervals, cities, start_date, end_date, metric):
    """Update the main chart based on selections using the data access layer."""
    # Get the data access layer
    data_access = get_weather_data_access()
    
    # Get filtered data directly from the data access layer
    df = data_access.get_weather_data_for_cities(cities, start_date, end_date)
    
    # If DataFrame is empty or missing necessary columns, return empty figure
    if df.empty or metric not in df.columns:
        return {
            'data': [],
            'layout': {
                'title': 'No data available',
                'xaxis': {'title': 'Timestamp'},
                'yaxis': {'title': metric.capitalize()},
                'template': 'plotly_white',
                'height': 400,
                'margin': {'l': 40, 'r': 40, 't': 60, 'b': 40}
            }
        }
    
    # Create the figure with improved styling
    fig = px.line(
        df, 
        x='timestamp', 
        y=metric, 
        color='city',
        title=f'{metric.capitalize()} Over Time',
        labels={
            'timestamp': 'Date & Time',
            metric: metric.capitalize(),
            'city': 'City'
        },
        template='plotly_white',
        line_shape='spline',  # Smooth lines
        render_mode='svg'     # Better for web display
    )
    
    # Enhance the figure appearance
    fig.update_layout(
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        hovermode='closest',
        height=400,
        margin={'l': 40, 'r': 40, 't': 60, 'b': 40}
    )
    
    # Add hover information
    fig.update_traces(
        hovertemplate='<b>%{customdata}</b><br>Date: %{x|%Y-%m-%d %H:%M}<br>' + 
                      f'{metric.capitalize()}: %{{y:.2f}}<extra></extra>',
        customdata=df['city']
    )
    
    return fig

@app.callback(
    Output('temperature-map', 'figure'),
    [Input('update-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [dash.dependencies.State('city-dropdown', 'value'),
     dash.dependencies.State('date-range', 'end_date')]
)
def update_temperature_map(n_clicks, n_intervals, cities, end_date):
    """Update the temperature map based on selections."""
    df = load_weather_data()
    
    # If DataFrame is empty or missing necessary columns, return empty figure
    if df.empty or 'city' not in df.columns or 'temperature' not in df.columns:
        return {
            'data': [],
            'layout': {
                'title': 'Temperature Map - No data available'
            }
        }
    
    # Filter by cities if selected
    if cities:
        df = df[df['city'].isin(cities)]
    
    # Filter to get the most recent data for each city
    if 'timestamp' in df.columns:
        # Get the latest date
        if end_date:
            latest_date = pd.to_datetime(end_date)
        else:
            latest_date = df['timestamp'].max()
        
        # Filter to data from the latest date
        df_latest = df[df['timestamp'] >= latest_date - timedelta(days=1)]
        
        # Get the latest record for each city
        df_latest = df_latest.sort_values('timestamp').drop_duplicates('city', keep='last')
    else:
        # If no timestamp, just take the first record for each city
        df_latest = df.drop_duplicates('city', keep='first')
    
    # If we have coordinates, create a scatter_geo plot
    if not df_latest.empty and 'coordinates' in df_latest.columns:
        # Extract lat and lon from coordinates
        if isinstance(df_latest['coordinates'].iloc[0], dict):
            df_latest['lat'] = df_latest['coordinates'].apply(lambda x: x.get('lat', 0) if isinstance(x, dict) else 0)
            df_latest['lon'] = df_latest['coordinates'].apply(lambda x: x.get('lon', 0) if isinstance(x, dict) else 0)
        
        # Ensure temperature values are positive for the size parameter
        df_latest['temperature_size'] = df_latest['temperature'].apply(lambda x: max(abs(x), 5))
        
        # Create scatter_geo plot
        fig = px.scatter_geo(
            df_latest,
            lat='lat',
            lon='lon',
            text='city',
            size='temperature_size',  # Use our positive temperature values for size
            color='temperature',      # Keep original temperature for color
            hover_name='city',
            hover_data=['temperature', 'weather_condition'],
            title='Latest Temperature by City',
            color_continuous_scale=px.colors.sequential.Plasma,
            size_max=30  # Limit the maximum size of markers
        )
        return fig
    
    # If no valid coordinates, create a simpler bar chart
    fig = px.bar(
        df_latest,
        x='city',
        y='temperature',
        title='Latest Temperature by City',
        color='temperature',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    
    return fig

@app.callback(
    Output('severity-chart', 'figure'),
    [Input('update-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [dash.dependencies.State('city-dropdown', 'value'),
     dash.dependencies.State('date-range', 'start_date'),
     dash.dependencies.State('date-range', 'end_date')]
)
def update_severity_chart(n_clicks, n_intervals, cities, start_date, end_date):
    """Update the weather severity chart based on selections."""
    df = load_weather_data()
    
    # If DataFrame is empty or missing necessary columns, return empty figure
    if df.empty or 'city' not in df.columns or 'weather_severity' not in df.columns:
        return {
            'data': [],
            'layout': {
                'title': 'Weather Severity - No data available'
            }
        }
    
    # Filter by cities if selected
    if cities:
        df = df[df['city'].isin(cities)]
    
    # Filter by date range if provided
    if start_date and end_date:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date) + timedelta(days=1)  # Include end date
        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
    
    if not df.empty:
        # Count occurrences of each severity level by city
        severity_counts = df.groupby(['city', 'weather_severity']).size().reset_index(name='count')
        
        # Create the stacked bar chart
        fig = px.bar(
            severity_counts, 
            x='city', 
            y='count', 
            color='weather_severity',
            title='Weather Severity Distribution by City',
            color_discrete_map={
                'normal': 'green',
                'mild': 'blue',
                'moderate': 'orange',
                'severe': 'red'
            }
        )
        
        return fig
    
    # Return empty figure if no data after filtering
    return {
        'data': [],
        'layout': {
            'title': 'Weather Severity - No data available for the selected criteria'
        }
    }

@app.callback(
    Output('alerts-table', 'children'),
    [Input('update-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')],
    [dash.dependencies.State('city-dropdown', 'value')]
)
def update_alerts_table(n_clicks, n_intervals, cities):
    """Update the weather alerts table."""
    df = load_weather_data()
    
    # If DataFrame is empty or missing necessary columns, return message
    if df.empty or 'city' not in df.columns or 'weather_severity' not in df.columns:
        return html.Div("No alert data available", style={'textAlign': 'center', 'padding': '20px'})
    
    # Filter by cities if selected
    if cities:
        df = df[df['city'].isin(cities)]
    
    # Get only severe weather data
    severe_weather = df[df['weather_severity'] == 'severe']
    
    if not severe_weather.empty:
        # Create the alerts table
        table_header = [
            html.Thead(html.Tr([
                html.Th("City"),
                html.Th("Condition"),
                html.Th("Temperature (°C)"),
                html.Th("Humidity (%)"),
                html.Th("Wind Speed"),
                html.Th("Timestamp")
            ]))
        ]
        
        rows = []
        for _, row in severe_weather.iterrows():
            rows.append(html.Tr([
                html.Td(row['city']),
                html.Td(row['weather_condition']),
                html.Td(f"{row['temperature']:.1f}"),
                html.Td(f"{row['humidity']:.1f}"),
                html.Td(f"{row['wind_speed']:.1f}"),
                html.Td(row['timestamp'])
            ]))
        
        table_body = [html.Tbody(rows)]
        
        return html.Table(table_header + table_body, 
                          style={'width': '100%', 'textAlign': 'left', 'borderCollapse': 'collapse'})
    
    return html.Div("No severe weather alerts", style={'textAlign': 'center', 'padding': '20px'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=DASHBOARD_PORT)
