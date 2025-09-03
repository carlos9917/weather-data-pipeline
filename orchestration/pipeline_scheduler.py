
import subprocess
import argparse
import sys
import os

def run_pipeline(date, cycle):
    """
    Runs the entire GFS data pipeline.

    Args:
        date (str): The date in YYYYMMDD format.
        cycle (str): The cycle ('00', '06', '12', '18').
    """
    scripts = [
        ("data_ingestion/gfs_downloader.py", "Downloading GFS data"),
        ("data_ingestion/met_downloader.py", "Downloading MET Nordic data"),
        ("data_processing/process_data.py", "Processing GFS data"),
        ("visualization/create_visualizations.py", "Creating GFS visualizations"),
        ("data_processing/process_met_data.py", "Processing MET data"),
        ("visualization/create_met_visualizations.py", "Creating MET visualizations")
    ]

    for script_path, description in scripts:
        print(f"--- {description} for {date} cycle {cycle} ---")
        script_abs_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), script_path)
        result = subprocess.run(
            [sys.executable, script_abs_path, "--date", date, "--cycle", cycle],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"Error running {script_path}:")
            print(result.stdout)
            print(result.stderr)
            sys.exit(1)
        else:
            print(result.stdout)
            print(f"--- Finished {description} ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the weather data pipeline.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    run_pipeline(args.date, args.cycle)
