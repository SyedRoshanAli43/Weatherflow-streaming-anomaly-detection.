#!/usr/bin/env python3
"""
Setup script for WeatherFlow project.
This script helps with initial project setup and configuration.
"""

import os
import sys
import shutil
import argparse
from pathlib import Path

def create_env_file():
    """Create a .env file from the template if it doesn't exist."""
    env_file = Path(".env")
    template_file = Path(".env.template")
    
    if not env_file.exists() and template_file.exists():
        shutil.copy(template_file, env_file)
        print("Created .env file from template. Please edit it to add your API keys.")
    elif not template_file.exists():
        print("Warning: .env.template file not found.")
    else:
        print(".env file already exists.")

def create_directories():
    """Create necessary directories if they don't exist."""
    dirs = [
        "data/raw",
        "data/processed",
        "data/reports"
    ]
    
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def check_dependencies():
    """Check if required system dependencies are installed."""
    dependencies = {
        "docker": "Docker is required for containerization",
        "docker-compose": "Docker Compose is required for orchestrating services",
        "python3": "Python 3 is required for running scripts"
    }
    
    missing = []
    
    for cmd, desc in dependencies.items():
        try:
            exit_code = os.system(f"which {cmd} > /dev/null 2>&1")
            if exit_code != 0:
                missing.append((cmd, desc))
        except Exception:
            missing.append((cmd, desc))
    
    if missing:
        print("Missing dependencies:")
        for cmd, desc in missing:
            print(f"  - {cmd}: {desc}")
        print("\nPlease install these dependencies before continuing.")
    else:
        print("All required dependencies are installed.")

def main():
    """Main function to set up the WeatherFlow project."""
    parser = argparse.ArgumentParser(description="Setup script for WeatherFlow project")
    parser.add_argument("--check-deps", action="store_true", help="Check required dependencies")
    parser.add_argument("--create-env", action="store_true", help="Create .env file from template")
    parser.add_argument("--create-dirs", action="store_true", help="Create required directories")
    parser.add_argument("--all", action="store_true", help="Run all setup tasks")
    
    args = parser.parse_args()
    
    # If no arguments provided or --all specified, run all tasks
    if len(sys.argv) == 1 or args.all:
        print("Running all setup tasks...\n")
        check_dependencies()
        print("")
        create_env_file()
        print("")
        create_directories()
        return
    
    # Otherwise, run specified tasks
    if args.check_deps:
        check_dependencies()
    
    if args.create_env:
        create_env_file()
    
    if args.create_dirs:
        create_directories()

if __name__ == "__main__":
    main()
