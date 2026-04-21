# WeatherFlow: Real-time Weather Data Pipeline

## Problem Statement

Weather data is critical for numerous applications including agriculture, transportation, urban planning, and emergency management. However, raw weather data often comes from diverse sources in varying formats, update frequencies, and quality levels. This creates several challenges:

1. **Data Fragmentation:** Weather data is scattered across multiple providers (OpenWeatherMap, WeatherAPI, weather stations) with different formats and access methods.

2. **Real-time Processing Requirements:** For many applications such as emergency response and transportation, weather data needs to be processed and analyzed in near real-time.

3. **Data Reliability and Quality:** Weather data can contain inconsistencies, missing values, or errors that need to be identified and addressed.

4. **Scalability Challenges:** Weather data collection and processing needs to scale efficiently across hundreds of locations and multiple data sources.

The WeatherFlow project aims to create a comprehensive data engineering solution to collect, process, and analyze weather data from multiple sources in real-time, addressing these challenges by building a robust, scalable pipeline.

## Dataset Description

The project uses three main data sources:

1. **OpenWeatherMap API:** Provides current weather data, forecasts, and historical data for cities worldwide. The API offers data on temperature, humidity, pressure, wind speed and direction, precipitation, and cloud coverage.

2. **WeatherAPI:** Delivers real-time and forecast weather data, including temperature, precipitation, wind conditions, and additional metrics like UV index and air quality. (API key is provided via the `WEATHERAPI_KEY` environment variable.)

3. **Simulated Weather Station Data:** The project includes a simulator that generates synthetic weather data to mimic IoT weather stations. This data includes the same metrics as the APIs but from fixed locations with continuous readings.

All three data sources are standardized to a common schema for consistent processing, containing fields such as:
- Location information (city, country, coordinates)
- Temperature (Celsius)
- Humidity (%)
- Atmospheric pressure (hPa)
- Wind speed and direction
- Weather conditions (clear, cloudy, rain, etc.)
- Timestamp of the reading

## Data Product Description

The WeatherFlow pipeline produces the following data products:

1. **Processed Dataset:** A unified, cleaned dataset of weather information from all sources, with derived metrics such as heat index calculations and weather severity categorization. This dataset is stored in both JSON and Parquet formats for different use cases.

2. **Real-time Weather Dashboard:** An interactive web dashboard built with Dash and Plotly that visualizes current weather conditions, trends, and anomalies across monitored locations.

3. **Automated Weather Reports:** Daily and weekly reports in multiple formats (CSV, HTML) summarizing weather patterns, highlighting extreme conditions, and providing statistical analysis of key metrics.

4. **Weather Alerts:** The system detects and flags severe weather conditions and generates alerts based on predefined thresholds for different weather parameters.

## Justification for Chosen Problem

The WeatherFlow project is an ideal data engineering challenge for several reasons:

1. **Complex ETL Requirements:** It involves extracting data from multiple sources, transforming it into a standardized format, and loading it into appropriate storage systems—demonstrating core data engineering competencies.

2. **Streaming Data Processing:** Weather data is continuous and time-sensitive, requiring robust stream processing solutions like Kafka and Spark Streaming.

3. **Workflow Orchestration:** The project demonstrates effective workflow management using Airflow to coordinate data collection, processing, and reporting tasks.

4. **Real-world Relevance:** Weather data has practical applications across multiple industries and directly impacts daily decisions, making this a meaningful problem to solve.

5. **Scalability Considerations:** The solution can scale to handle more locations, additional data sources, and increased processing requirements.

6. **Multiple Technology Integration:** The project integrates various data engineering technologies (Kafka, Spark, Airflow, Docker) to create a comprehensive solution.

The WeatherFlow project goes beyond simple data processing by implementing a complete data engineering pipeline that addresses real-world challenges and produces valuable data products for end-users.

## Student Information

- Student ID: 2021648
- Student ID: 2021127
- Course: DS463 Data Engineering
- Date: May 2025
