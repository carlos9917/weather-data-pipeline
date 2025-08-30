#!/bin/bash

# run.sh

# This script provides a simple interface for running the GFS Wind Power Density Pipeline.

# Function to display help
show_help() {
    echo "Usage: ./run.sh [scheduler|dashboard|dashboard-py|manual|default] [options]"
    echo "  - scheduler: Run the automated data extraction and analysis pipeline."
    echo "  - dashboard: Run the interactive R Shiny dashboard."
    echo "  - dashboard-py: Run the interactive Python Dash dashboard."
    echo "  - manual: Run a one-time data extraction and analysis."
    echo "    - --date <YYYYMMDD>: The date to extract data for."
    echo "    - --cycle <00|06|12|18>: The GFS cycle to extract data for."
    echo "  - default: Run the original pipeline script."
}


## Activate virtual environment if it exists
#if [ -f ".venv/bin/activate" ]; then
#    source .venv/bin/activate
#fi

# Get the first argument to determine the mode
MODE=$1
shift

# Display help if no arguments are provided
if [ -z "$MODE" ]; then
    show_help
    exit 0
fi


# --- Scheduler Mode ---
if [ "$MODE" == "scheduler" ]; then
    echo "Starting the scheduler..."
    echo "Cleaning up old index files..."
    find data/raw -name "*.idx" -delete
    source .venv/bin/activate
    python3 src/scheduler.py --mode scheduler
    
# --- Dashboard Mode ---
elif [ "$MODE" == "dashboard" ]; then
    echo "Starting the dashboard..."
    python src/dashboard.py
    
# --- Manual Mode ---
elif [ "$MODE" == "manual" ]; then
    echo "Running manual data extraction..."
    source .venv/bin/activate
    python3 src/scheduler.py --mode manual "$@"
    
# --- Original Mode (for compatibility) ---
elif [ "$MODE" == "default" ]; then
    # Get today's date in YYYYMMDD format
    DATE=$(date +%Y%m%d)
    # Get the previous cycle (00, 06, 12, 18)
    # This is a simple example; a robust implementation would check for availability
    HOUR=$(date +%H)
    # Remove leading zero to avoid octal interpretation
    HOUR_NUM=$((10#$HOUR))
    echo $DATE $HOUR
    if (( HOUR_NUM >= 0 && HOUR_NUM < 6 )); then CYCLE="18"; DATE=$(date -d "yesterday" +%Y%m%d); fi

    if (( HOUR_NUM >= 6 && HOUR_NUM < 12 )); then CYCLE="00"; fi

    if (( HOUR_NUM >= 12 && HOUR_NUM < 18 )); then CYCLE="06"; fi

    if (( HOUR_NUM >= 18 )); then CYCLE="12"; fi

    echo "Running pipeline for date $DATE and cycle $CYCLE"

    # Run the Python data extraction script
    source .venv/bin/activate
    python3 src/data_extractor.py --date $DATE --cycle $CYCLE


    # Check if the python script succeeded
    if [ $? -eq 0 ]; then
      echo "data extractor script finished successfully."
      # Run the python analysis script
      python src/analysis.py --date $DATE --cycle $CYCLE
      if [ $? -eq 0 ]; then
        echo "R script finished successfully."
      else
        echo "R script failed. Halting pipeline."
        exit 1
      fi
    else
      echo "Python script failed. Halting pipeline."
      exit 1
    fi

    echo "Pipeline finished."

# --- Help ---
else
    show_help
fi
