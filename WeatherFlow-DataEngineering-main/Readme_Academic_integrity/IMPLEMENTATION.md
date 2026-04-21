# WeatherFlow: Implementation Guide

This document provides detailed information about the implementation of the WeatherFlow project, including the architecture, components, and how to run the system.

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Component Details](#component-details)
3. [Setup Instructions](#setup-instructions)
4. [Running the Pipeline](#running-the-pipeline)
5. [Monitoring and Debugging](#monitoring-and-debugging)
6. [Extending the Project](#extending-the-project)

## Architecture Overview

The WeatherFlow system is built with a modern data engineering architecture that includes:

### Data Flow
1. **Data Ingestion**: Python scripts fetch data from weather APIs (OpenWeatherMap and WeatherAPI)
2. **Data Streaming**: Kafka streams the data in real-time between components
3. **Data Processing**: Apache Spark processes and analyzes the weather data
4. **Workflow Orchestration**: Apache Airflow schedules and monitors the entire pipeline
5. **Data Storage**: Files are stored in a data lake architecture (raw and processed zones)
6. **Visualization**: Interactive Dash/Flask dashboard visualizes insights

### Technology Stack
- **Languages**: Python, SQL, Bash
- **Frameworks**: Apache Spark, Apache Kafka, Apache Airflow, Flask/Dash
- **Storage**: File-based storage with a data lake structure
- **Containerization**: Docker with Docker Compose for easy deployment

## Component Details

### 1. Data Ingestion (`scripts/weather_api.py`)
- Fetches data from multiple weather APIs
- Standardizes data formats across sources
- Saves raw data locally for backup
- Configured via `scripts/config.py`

### 2. Kafka Streaming (`kafka/scripts/`)
- **Producer** (`kafka/scripts/producer.py`): Streams weather data to Kafka
- **Consumer** (`kafka/scripts/consumer.py`): Processes streamed data
- **Monitor** (`kafka/scripts/monitor.py`): Helps debug Kafka topics

### 3. Spark Processing (`spark/scripts/weather_processor.py`)
- Processes weather data in batch mode
- Implements streaming analytics
- Calculates derived metrics (heat index, weather severity)
- Optimized for distributed processing

### 4. Airflow Orchestration (`airflow/dags/weather_pipeline_dag.py`)
- Schedules the entire pipeline
- Monitors the health of each component
- Handles retries and failure scenarios
- Supports both scheduled and triggered runs

### 5. Data Reporting (`scripts/generate_reports.py`)
- Creates daily weather reports
- Generates visualizations for key metrics
- Outputs to multiple formats (CSV, HTML, PNG)

### 6. Dashboard (`dashboard/app.py`)
- Real-time weather visualization
- Interactive filtering and exploration
- Multiple chart types for different metrics
- Responsive design for different devices

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- API keys from [OpenWeatherMap](https://openweathermap.org/api) and [WeatherAPI](https://www.weatherapi.com/)

### Initial Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd weather_flow
   ```

2. Run the setup script:
   ```bash
   python setup.py --all
   ```

3. Edit the `.env` file with your API keys:
   ```
   OPENWEATHER_API_KEY=your_openweather_api_key
   WEATHERAPI_KEY=your_weatherapi_key
   ```

4. Create the necessary data directories:
   ```bash
   mkdir -p data/raw data/processed data/reports
   ```

## Running the Pipeline

### Using Docker (Recommended)

1. Start all services with Docker Compose:
   ```bash
   cd docker
   docker-compose up -d
   ```

2. Monitor the services:
   ```bash
   docker-compose ps
   docker-compose logs -f
   ```

3. Access the dashboard:
   - Open `http://localhost:8051` in your browser

4. Access Airflow:
   - Open `http://localhost:8091` in your browser
   - Login with username `admin` and password `admin`

### Running Components Individually

1. Start Kafka (required for streaming):
   ```bash
   docker-compose up -d zookeeper kafka
   ```

2. Run the weather data producer:
   ```bash
   python kafka/scripts/producer.py
   ```

3. Run the weather data consumer:
   ```bash
   python kafka/scripts/consumer.py
   ```

4. Process data with Spark:
   ```bash
   python spark/scripts/weather_processor.py
   ```

5. Generate reports:
   ```bash
   python scripts/generate_reports.py
   ```

6. Launch the dashboard:
   ```bash
   python dashboard/app.py
   ```

## Monitoring and Debugging

### Kafka Monitoring

Monitor Kafka topics using the monitor script:

```bash
# List all topics
python kafka/scripts/monitor.py --list-topics

# Monitor raw weather data
python kafka/scripts/monitor.py --topic weather-raw-data

# Monitor processed weather data
python kafka/scripts/monitor.py --topic weather-processed-data
```

### Checking Data Files

Verify data files are being created properly:

```bash
# Check raw data files
ls -la data/raw/

# Check processed data files
ls -la data/processed/

# Check report files
ls -la data/reports/
```

### Airflow Monitoring

Access the Airflow web UI to monitor DAG runs:
- URL: http://localhost:8091
- Username: admin
- Password: admin

## Extending the Project

### Adding New Data Sources

1. Update `scripts/config.py` with new API endpoints
2. Implement a new fetcher function in `scripts/weather_api.py`
3. Add a parser function in `scripts/utils.py` for the new data format

### Creating New Analytics

1. Add new processing logic in `spark/scripts/weather_processor.py`
2. Update the report generation in `scripts/generate_reports.py`
3. Add new visualizations to the dashboard in `dashboard/app.py`

### Scaling the System

For larger deployments:
1. Increase Kafka partitions for better throughput
2. Add more Spark workers for distributed processing
3. Set up proper monitoring and alerting
4. Consider cloud deployment for elastic scaling
