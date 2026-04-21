#!/bin/bash
# Script to run WeatherFlow components in standalone mode (without Docker)

# Set up colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_message() {
    echo -e "${BLUE}[WeatherFlow]${NC} $1"
}

print_step() {
    echo -e "\n${GREEN}===== $1 =====${NC}"
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

# Check if .env file exists
if [ ! -f .env ]; then
    print_warning ".env file not found. Creating from template..."
    cp .env.template .env
    print_warning "Please edit the .env file to add your API keys."
fi

# Source .env file to get environment variables
set -a
source .env
set +a

# Create necessary directories
print_step "Creating directories"
mkdir -p data/raw data/processed data/reports
print_message "Directories created."

# Check Python installation
print_step "Checking Python installation"
if command -v python3 &>/dev/null; then
    PYTHON=python3
elif command -v python &>/dev/null; then
    PYTHON=python
else
    print_error "Python not found. Please install Python 3."
    exit 1
fi
print_message "Using Python: $($PYTHON --version)"

# Install requirements
print_step "Installing requirements"
$PYTHON -m pip install -r requirements.txt
if [ $? -ne 0 ]; then
    print_error "Failed to install requirements."
    exit 1
fi
print_message "Requirements installed."

# Choose component to run
print_step "Select component to run"
echo "1) Weather API Fetcher"
echo "2) Dashboard"
echo "3) Generate Reports"
echo "4) Process Data with Spark"
echo "5) Run Weather API Fetcher in continuous mode"
echo "q) Quit"

read -p "Enter your choice: " choice

case $choice in
    1)
        print_step "Running Weather API Fetcher"
        $PYTHON scripts/weather_api.py
        ;;
    2)
        print_step "Running Dashboard"
        $PYTHON dashboard/app.py
        ;;
    3)
        print_step "Generating Reports"
        $PYTHON scripts/generate_reports.py
        ;;
    4)
        print_step "Processing Data with Spark"
        $PYTHON spark/scripts/weather_processor.py
        ;;
    5)
        print_step "Running Weather API Fetcher in continuous mode"
        while true; do
            print_message "Fetching weather data..."
            $PYTHON scripts/weather_api.py
            print_message "Sleeping for 5 minutes..."
            sleep 300
        done
        ;;
    q)
        print_message "Exiting..."
        exit 0
        ;;
    *)
        print_error "Invalid choice."
        exit 1
        ;;
esac
