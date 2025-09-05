#!/bin/bash
set -e

# Default values
MODE="default"

# Parse command-line arguments
if [ -n "$1" ]; then
    MODE=$1
    shift
fi

# Execute the appropriate command
case "$MODE" in
    "scheduler")
        echo "Starting the scheduler..."
        exec python3 orchestration/pipeline_scheduler.py
        ;;
    "dashboard")
        echo "Starting the Python Dash dashboard..."
        exec python3 visualization/run_dashboard.py
        ;;
    "manual")
        echo "Running manual data extraction..."
        exec python3 orchestration/pipeline_scheduler.py "$@"
        ;;
    "default")
        echo "Running the pipeline for the latest available data..."
        exec python3 orchestration/pipeline_scheduler.py
        ;;
    *)
        echo "Unknown command: $MODE"
        echo "Usage: [scheduler|dashboard|manual|default]"
        exit 1
        ;;
esac
