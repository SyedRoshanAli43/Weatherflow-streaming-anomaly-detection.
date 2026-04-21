#!/usr/bin/env python3
"""
Main script to run the complete WeatherFlow pipeline for demonstration purposes.
This script simulates a full execution of the data pipeline without requiring Docker or Kafka.
"""

import os
import sys
import time
import logging
import argparse
from pathlib import Path
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.absolute()
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SPARK_DIR = PROJECT_ROOT / "spark" / "scripts"
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

def ensure_directories():
    """Ensure all necessary directories exist."""
    dirs = [
        DATA_DIR / "raw",
        DATA_DIR / "processed",
        REPORTS_DIR
    ]
    
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")

def run_command(cmd, cwd=None, env=None):
    """Run a shell command and log output."""
    logger.info(f"Running command: {cmd}")
    
    try:
        # Set up environment with current env plus any additional variables
        cmd_env = os.environ.copy()
        if env:
            cmd_env.update(env)
        
        # Run the command
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd,
            env=cmd_env
        )
        
        # Stream output in real-time
        while True:
            stdout_line = process.stdout.readline()
            stderr_line = process.stderr.readline()
            
            if stdout_line:
                logger.info(stdout_line.strip())
            
            if stderr_line:
                logger.error(stderr_line.strip())
            
            # Check if process has finished
            if process.poll() is not None:
                # Read any remaining output
                for line in process.stdout:
                    logger.info(line.strip())
                for line in process.stderr:
                    logger.error(line.strip())
                break
        
        # Check return code
        if process.returncode != 0:
            logger.error(f"Command failed with exit code {process.returncode}")
            return False
        
        return True
    
    except Exception as e:
        logger.error(f"Error running command: {e}")
        return False

def collect_weather_data():
    """Collect weather data from APIs and simulated weather stations."""
    logger.info("Step 1: Collecting weather data")
    
    # Run the weather API collector
    api_success = run_command(f"python {SCRIPTS_DIR}/weather_api.py", cwd=PROJECT_ROOT)
    
    # Run the weather station simulator
    simulator_success = run_command(f"python {SCRIPTS_DIR}/weather_simulator.py --single", cwd=PROJECT_ROOT)
    
    return api_success and simulator_success

def process_data_with_spark():
    """Process the collected data using Spark."""
    logger.info("Step 2: Processing data with Spark")
    
    return run_command(f"python {SPARK_DIR}/weather_processor.py", cwd=PROJECT_ROOT)

def generate_reports():
    """Generate reports from the processed data."""
    logger.info("Step 3: Generating reports")
    
    return run_command(f"python {SCRIPTS_DIR}/generate_reports.py", cwd=PROJECT_ROOT)

def start_dashboard(blocking=False):
    """Start the dashboard server."""
    logger.info("Step 4: Starting dashboard")
    
    if blocking:
        return run_command(f"python {PROJECT_ROOT}/dashboard/app.py", cwd=PROJECT_ROOT)
    else:
        cmd = f"python {PROJECT_ROOT}/dashboard/app.py"
        logger.info(f"To start the dashboard, run: {cmd}")
        return True

def run_complete_pipeline(include_dashboard=False):
    """Run the complete pipeline."""
    logger.info("Starting the WeatherFlow pipeline")
    
    # Ensure directories exist
    ensure_directories()
    
    # Step 1: Collect data
    if not collect_weather_data():
        logger.error("Data collection failed")
        return False
    
    # Step 2: Process data
    if not process_data_with_spark():
        logger.error("Data processing failed")
        return False
    
    # Step 3: Generate reports
    if not generate_reports():
        logger.error("Report generation failed")
        return False
    
    # Step 4: Start dashboard if requested
    if include_dashboard:
        if not start_dashboard(blocking=True):
            logger.error("Dashboard startup failed")
            return False
    else:
        start_dashboard(blocking=False)
    
    logger.info("Pipeline execution completed successfully")
    return True

def run_continuous_pipeline(interval_minutes=60, include_dashboard=False):
    """Run the pipeline continuously at specified intervals."""
    logger.info(f"Starting continuous pipeline execution (interval: {interval_minutes} minutes)")
    
    # Ensure directories exist
    ensure_directories()
    
    try:
        iteration = 1
        
        # Start dashboard in a separate process if requested
        dashboard_process = None
        if include_dashboard:
            try:
                logger.info("Starting dashboard in background")
                dashboard_cmd = f"python {PROJECT_ROOT}/dashboard/app.py"
                dashboard_process = subprocess.Popen(
                    dashboard_cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=PROJECT_ROOT
                )
                logger.info("Dashboard started in background")
            except Exception as e:
                logger.error(f"Error starting dashboard: {e}")
        
        # Main loop
        while True:
            logger.info(f"Starting iteration {iteration}")
            
            # Step 1: Collect data
            collect_weather_data()
            
            # Step 2: Process data
            process_data_with_spark()
            
            # Step 3: Generate reports
            generate_reports()
            
            # Increment iteration counter
            iteration += 1
            
            # Wait for next interval
            interval_seconds = interval_minutes * 60
            logger.info(f"Waiting {interval_minutes} minutes until next iteration...")
            time.sleep(interval_seconds)
    
    except KeyboardInterrupt:
        logger.info("Pipeline execution stopped by user")
    except Exception as e:
        logger.error(f"Error in continuous pipeline execution: {e}")
    finally:
        # Clean up dashboard process if it was started
        if dashboard_process:
            logger.info("Stopping dashboard process")
            dashboard_process.terminate()
            dashboard_process.wait()

def main():
    """Main function to run the pipeline."""
    parser = argparse.ArgumentParser(description='WeatherFlow Pipeline Runner')
    parser.add_argument('--continuous', action='store_true', help='Run the pipeline continuously')
    parser.add_argument('--interval', type=int, default=60, help='Interval in minutes for continuous mode')
    parser.add_argument('--dashboard', action='store_true', help='Start the dashboard')
    parser.add_argument('--collect-only', action='store_true', help='Only collect data')
    parser.add_argument('--process-only', action='store_true', help='Only process existing data')
    parser.add_argument('--reports-only', action='store_true', help='Only generate reports')
    
    args = parser.parse_args()
    
    # Handle single-task modes
    if args.collect_only:
        return collect_weather_data()
    
    if args.process_only:
        return process_data_with_spark()
    
    if args.reports_only:
        return generate_reports()
    
    # Run in continuous or one-time mode
    if args.continuous:
        run_continuous_pipeline(args.interval, args.dashboard)
    else:
        run_complete_pipeline(args.dashboard)

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
