#!/bin/bash

# run.sh

# This script provides a simple interface for running the GFS Wind Power Density Pipeline.

# Function to display help
show_help() {
    echo "Usage: ./run.sh [scheduler|manual|default] [options]"
    echo "  - scheduler: Not yet implemented."
    echo "  - manual: Run a one-time data extraction and analysis."
    echo "    - --date <YYYYMMDD>: The date to extract data for."
    echo "    - --cycle <00|06|12|18>: The GFS cycle to extract data for."
    echo "  - default: Run the pipeline for the latest available data."
}


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
    echo "Scheduler mode is running..."
    while true; do
        DATE=$(date +%Y%m%d)
        HOUR=$(date +%H)
        HOUR_NUM=$((10#$HOUR))
        if (( HOUR_NUM >= 0 && HOUR_NUM < 6 )); then CYCLE="18"; DATE=$(date -d "yesterday" +%Y%m%d); fi
        if (( HOUR_NUM >= 6 && HOUR_NUM < 12 )); then CYCLE="00"; fi
        if (( HOUR_NUM >= 12 && HOUR_NUM < 18 )); then CYCLE="06"; fi
        if (( HOUR_NUM >= 18 )); then CYCLE="12"; fi

        echo "Running pipeline for date $DATE and cycle $CYCLE"
        python3 orchestration/pipeline_scheduler.py --date $DATE --cycle $CYCLE

        if [ $? -eq 0 ]; then
            echo "Pipeline finished successfully."
        else
            echo "Pipeline failed."
        fi
        echo "Waiting for 6 hours before next run..."
        sleep 21600
    done
    
# --- Manual Mode ---
elif [ "$MODE" == "manual" ]; then
    echo "Running manual data extraction..."
    python3 orchestration/pipeline_scheduler.py "$@"
    
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
    python3 orchestration/pipeline_scheduler.py --date $DATE --cycle $CYCLE


    # Check if the python script succeeded
    if [ $? -eq 0 ]; then
      echo "Pipeline finished successfully."
    else
      echo "Pipeline failed."
      exit 1
    fi

# --- Help ---
else
    show_help
fi
