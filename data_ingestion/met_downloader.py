
import os
import requests
import argparse
from datetime import datetime
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    MET_NORDIC_BASE_URL,
    MET_NORDIC_FILE_TEMPLATE
)

def download_met_data(date_str, cycle):
    """
    Downloads MET Nordic data for a specific date and cycle.

    Args:
        date_str (str): The date in YYYYMMDD format.
        cycle (str): The cycle, e.g., '06' for T06Z.
    """
    # The URL is structured with year, month, day.
    # Let's parse the date_str to get these components.
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"Error: Invalid date format for {date_str}. Please use YYYYMMDD.")
        return

    raw_data_dir = os.path.join('data', 'raw', 'met', date_str, cycle)
    os.makedirs(raw_data_dir, exist_ok=True)

    file_name = MET_NORDIC_FILE_TEMPLATE.format(year=year, month=month, day=day, date=date_str, cycle=cycle)
    file_path = os.path.join(raw_data_dir, file_name)

    if os.path.exists(file_path):
        print(f"File already exists: {file_path}")
        return

    # Construct the full URL
    url = f"{MET_NORDIC_BASE_URL}/{year}/{month}/{day}/{file_name}"

    try:
        print(f"Downloading from: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded: {file_path}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {file_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download MET Nordic data.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle, e.g., 06 for T06Z.")
    args = parser.parse_args()

    # For now, we know that the URL contains T06Z, so we are expecting '06'.
    # We can add a check here.
    if args.cycle != '06':
        print("Warning: Currently, only cycle '06' is supported for MET Nordic data.")
        # Or sys.exit(1) if we want to be strict.
    
    download_met_data(args.date, args.cycle)
