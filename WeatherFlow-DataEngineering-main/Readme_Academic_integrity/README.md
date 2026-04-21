# WeatherFlow: Real-time Weather Data Pipeline

## Project Overview
WeatherFlow is an end-to-end data engineering solution for collecting, processing, and analyzing real-time weather data from multiple sources. This project demonstrates the application of data engineering concepts including data ingestion, streaming, processing, storage, and visualization for the DS463 Data Engineering final project.

## Problem Statement
Weather data is critical for various applications such as agriculture, transportation, emergency services, and urban planning. However, raw weather data often comes from multiple sources in different formats and frequencies. This project aims to create a unified pipeline that can:
- Ingest data from multiple weather API sources
- Process and transform the data in real-time
- Store both raw and processed data efficiently
- Provide meaningful insights through visualizations
- Orchestrate the entire workflow automatically

## Architecture

The pipeline consists of the following components:
1. **Data Ingestion**: REST API calls to weather data providers (WeatherAPI and OpenWeatherMap)
2. **Message Broker**: Apache Kafka for real-time data streaming
3. **Data Processing**: Apache Spark for data transformation and analysis
4. **Workflow Orchestration**: Apache Airflow for scheduling and monitoring
5. **Data Storage**: Data lake architecture with raw and processed zones
6. **Visualization**: Interactive Flask dashboard for weather insights

## Technologies Used
- **Apache Airflow**: Workflow orchestration
- **Apache Kafka**: Real-time data streaming
- **Apache Spark**: Distributed data processing
- **PostgreSQL**: Database for Airflow metadata
- **Docker**: Containerization
- **Python**: Programming language
- **Pandas**: Data manipulation
- **Flask**: Web dashboard
- **ApexCharts.js**: Interactive data visualization

## Project Structure
```
weather_flow/
├── airflow/             # Airflow DAGs and plugins
├── dashboard/           # Flask dashboard for visualization
├── data/                # Data storage (raw, processed, reports)
├── docker/              # Docker configuration files
├── kafka/               # Kafka producers and consumers
├── scripts/             # Weather API and utility scripts
├── spark/               # Spark processing jobs
├── tests/               # Unit and integration tests
├── README.md            # Project documentation
└── requirements.txt     # Python dependencies
```

## Setup and Installation

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- WeatherAPI key (set via `WEATHERAPI_KEY`)
- OpenWeatherMap API key (optional; set via `OPENWEATHER_API_KEY`)

### Installation Steps

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd weather_flow
   ```

2. Create a `.env` file with your API keys:
   ```
   WEATHERAPI_KEY=your_weatherapi_key
   OPENWEATHER_API_KEY=your_openweather_api_key
   ```

3. Start all services with Docker Compose:
   ```bash
   cd docker
   docker-compose up -d
   ```

## Running the Pipeline

### Using Docker (Recommended)

1. Start all services:
   ```bash
   cd weather_flow/docker
   docker-compose up -d
   ```

2. Monitor the services:
   ```bash
   docker-compose ps
   docker-compose logs -f
   ```

3. Access the components:
   - **Dashboard**: http://localhost:8051
   - **Airflow UI**: http://localhost:8091 (username: admin, password: admin)
   - **Kafka UI (Kafdrop)**: http://localhost:9000
   - **Spark Master UI**: http://localhost:8081

### Running the Dashboard Locally (Outside Docker)

If you want to run the dashboard locally:

```bash
cd weather_flow
python streamlit_dashboard.py
```

The dashboard will be available at http://localhost:8501

### Verifying Components

1. Verify Kafka Producer and Consumer:
   ```bash
   # View Kafka producer logs
   docker logs weather-producer
   
   # View Kafka consumer logs
   docker logs weather-consumer
   
   # View Kafka topics
   open http://localhost:9000
   ```

2. Verify PostgreSQL:
   ```bash
   # Check PostgreSQL connection
   docker exec postgres psql -U airflow -d airflow -c "SELECT version();"
   
   # Create a test table and insert data
   docker exec postgres psql -U airflow -d airflow -c "CREATE TABLE IF NOT EXISTS weather_test (id SERIAL PRIMARY KEY, city VARCHAR(100), temperature FLOAT, humidity INTEGER, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
   
   docker exec postgres psql -U airflow -d airflow -c "INSERT INTO weather_test (city, temperature, humidity) VALUES ('London', 15.3, 48), ('Berlin', 14.3, 44), ('New York', 18.3, 81), ('Tokyo', 19.3, 94), ('Sydney', 22.5, 65);"
   
   # Query the data
   docker exec postgres psql -U airflow -d airflow -c "SELECT city, temperature, humidity FROM weather_test ORDER BY temperature DESC;"
   ```

3. Verify Spark:
   ```bash
   # Check Spark version
   docker exec spark-master spark-submit --version
   
   # View Spark Master UI
   open http://localhost:8081
   ```

4. Test Weather API:
   ```bash
   # Test WeatherAPI connection (requires WEATHERAPI_KEY env var)
   python -c "import os,requests; api_key=os.environ['WEATHERAPI_KEY']; r=requests.get('http://api.weatherapi.com/v1/current.json', params={'key': api_key, 'q': 'London', 'aqi': 'no'}); print(r.json())"
   ```

## Data Sources
- [WeatherAPI](https://www.weatherapi.com/) - Primary data source (API key via `WEATHERAPI_KEY`)
- [OpenWeatherMap API](https://openweathermap.org/api) - Secondary data source (API key via `OPENWEATHER_API_KEY`)
- Simulated weather station data

## Adding New Cities

You can add new cities to the dashboard through the UI:
1. Navigate to the "Data" tab in the dashboard
2. Use the "Add New City" form to enter a city name and country
3. Click "Add City" to fetch and display data for the new location

## Docker Configuration

The project uses specific Docker configuration to ensure all services work correctly:
1. Airflow uses PostgreSQL instead of SQLite for production use
2. Scripts directory is mounted to /opt/airflow/scripts for DAG access
3. Dashboard binds to 0.0.0.0 on port 8051
4. All services use proper network configuration via weather-network
5. Port mappings: Airflow (8091:8080), Dashboard (8051:8051), Spark (8081:8080), Kafka UI (9000:9000)

## Contributors
- Student ID: 2021648
- Student ID: 2021127