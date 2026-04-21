#!/usr/bin/env python3
"""
Script to check Docker logs for Kafka producer, consumer, and Spark components
"""

import subprocess
import sys

def run_command(command):
    """Run a shell command and return the output"""
    try:
        result = subprocess.run(command, shell=True, check=True, 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               universal_newlines=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command '{command}': {e}")
        print(f"Error output: {e.stderr}")
        return None

def check_container_logs(container_name, lines=20):
    """Check logs for a specific container"""
    print(f"\n{'='*80}")
    print(f"Logs for {container_name} (last {lines} lines):")
    print(f"{'='*80}")
    
    logs = run_command(f"docker logs {container_name} --tail {lines}")
    if logs:
        print(logs)
    else:
        print(f"No logs available for {container_name}")

def main():
    print("Checking Docker logs for WeatherFlow components...")
    
    # Check if containers are running
    containers = run_command("docker ps --format '{{.Names}}'")
    if not containers:
        print("No Docker containers found or error running docker ps")
        return
    
    container_list = containers.split('\n')
    
    # Define containers to check
    check_containers = [
        {"name": "weather-producer", "description": "Kafka Producer"},
        {"name": "weather-consumer", "description": "Kafka Consumer"},
        {"name": "spark-master", "description": "Spark Master"},
        {"name": "spark-worker", "description": "Spark Worker"},
        {"name": "kafka", "description": "Kafka Broker"},
        {"name": "zookeeper", "description": "ZooKeeper"}
    ]
    
    # Check logs for each container
    for container in check_containers:
        if container["name"] in container_list:
            print(f"\nChecking {container['description']} ({container['name']})...")
            check_container_logs(container["name"])
        else:
            print(f"\n{container['description']} ({container['name']}) is not running")
    
    # Print URLs for web interfaces
    print("\n\nWeb interfaces for monitoring:")
    print("  - Kafka UI (Kafdrop): http://localhost:9000")
    print("  - Spark Master UI: http://localhost:8081")
    print("  - Airflow UI: http://localhost:8091")
    print("  - WeatherFlow Dashboard (Docker): http://localhost:8051")
    print("  - WeatherFlow Dashboard (Local): http://localhost:8501")

if __name__ == "__main__":
    main()
