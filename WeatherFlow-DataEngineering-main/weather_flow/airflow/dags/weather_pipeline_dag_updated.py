"""
Airflow DAG for the WeatherFlow pipeline.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

# Add the current directory to the path so we can import from the local scripts directory
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Define paths directly to avoid import issues
RAW_DATA_PATH = '/opt/airflow/data/raw'
PROCESSED_DATA_PATH = '/opt/airflow/data/processed'

# We'll import the actual functions at runtime in the tasks
# This avoids import errors during DAG parsing
def fetch_weather_data_wrapper(**kwargs):
    """Wrapper function to import and call the weather API function at runtime"""
    import time
    import importlib.util
    import sys
    
    # Add a delay to avoid resource contention
    time.sleep(2)
    
    # Dynamically import the module at runtime
    try:
        spec = importlib.util.spec_from_file_location(
            "weather_api", 
            "/opt/airflow/weather_scripts/weather_api.py"
        )
        weather_api = importlib.util.module_from_spec(spec)
        sys.modules["weather_api"] = weather_api
        spec.loader.exec_module(weather_api)
        
        # Call the main function
        return weather_api.main(**kwargs)
    except Exception as e:
        print(f"Error importing or running weather_api: {e}")
        raise

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 1),
}

# Define the DAG
dag = DAG(
    'weather_flow_pipeline',
    default_args=default_args,
    description='Pipeline for processing weather data',
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

# Task to fetch weather data from APIs
fetch_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

# Task to ensure data directory exists
create_dirs_task = BashOperator(
    task_id='create_directories',
    bash_command=f'mkdir -p {RAW_DATA_PATH} {PROCESSED_DATA_PATH}',
    dag=dag,
)

# Wait for data to be available
wait_for_data_task = FileSensor(
    task_id='wait_for_data',
    filepath=RAW_DATA_PATH,
    poke_interval=60,  # Check every minute
    timeout=3600,  # Timeout after an hour
    mode='reschedule',  # Don't block a worker slot while waiting
    dag=dag,
)

# Process data with Python instead of Spark
process_data_task = BashOperator(
    task_id='process_weather_data',
    bash_command=f'python /opt/airflow/scripts/process_weather_data.py --input {RAW_DATA_PATH} --output {PROCESSED_DATA_PATH}',
    dag=dag,
)

# Generate daily reports
generate_reports_task = BashOperator(
    task_id='generate_reports',
    bash_command=f'python /opt/airflow/scripts/generate_reports.py',  # Simplified path
    dag=dag,
)

# Clean up old data (keep only 30 days)
cleanup_task = BashOperator(
    task_id='cleanup_old_data',
    bash_command=f'find {RAW_DATA_PATH} -type f -mtime +30 -delete && find {PROCESSED_DATA_PATH} -type f -mtime +30 -delete',
    dag=dag,
)

# Define the workflow
create_dirs_task >> fetch_data_task >> wait_for_data_task >> process_data_task >> generate_reports_task >> cleanup_task
