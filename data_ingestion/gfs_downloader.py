
import os
import requests
import argparse
from datetime import datetime
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    NOMADS_GRIB_FILTER_URL,
    GFS_FILE_TEMPLATE,
    EUROPE_BOUNDS,
    FORECAST_HOURS,
    GFS_VARIABLES
)

def download_gfs_data(date_str, cycle):
    """
    Downloads GFS data for a specific date and cycle.

    Args:
        date_str (str): The date in YYYYMMDD format.
        cycle (str): The cycle ('00', '06', '12', '18').
    """
    raw_data_dir = os.path.join('data', 'raw', 'gfs', date_str, cycle)
    os.makedirs(raw_data_dir, exist_ok=True)

    for forecast_hour in FORECAST_HOURS:
        file_name = GFS_FILE_TEMPLATE.format(cycle=cycle, forecast_hour=forecast_hour)
        file_path = os.path.join(raw_data_dir, file_name)

        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            continue

        params = {
            'file': file_name,
            'lev_100_m_above_ground': 'on',
            'var_UGRD': 'on',
            'var_VGRD': 'on',
            'lev_2_m_above_ground': 'on',
            'var_TMP': 'on',
            'subregion': '',
            'leftlon': str(EUROPE_BOUNDS['lon_min']),
            'rightlon': str(EUROPE_BOUNDS['lon_max']),
            'toplat': str(EUROPE_BOUNDS['lat_max']),
            'bottomlat': str(EUROPE_BOUNDS['lat_min']),
            'dir': f'/gfs.{date_str}/{cycle}/atmos'
        }

        try:
            response = requests.get(NOMADS_GRIB_FILTER_URL, params=params, stream=True)
            response.raise_for_status()

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"Downloaded: {file_path}")

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {file_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download GFS data.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    download_gfs_data(args.date, args.cycle)
