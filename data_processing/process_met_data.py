import os
import xarray as xr
import argparse
import sys
import numpy as np
import warnings
import zarr

warnings.filterwarnings("ignore")

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_FORMAT

def process_met_data_zarr(date_str, cycle):
    """
    Processes raw MET data and appends it to a Zarr store, optimized for large files.
    """
    raw_data_dir = os.path.join('data', 'raw', 'met', date_str, cycle)
    ZARR_STORE_PATH_MET = f"data/processed/met_data_{date_str}_{cycle}.zarr"
    os.makedirs(os.path.dirname(ZARR_STORE_PATH_MET), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return

    all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                 if f.endswith(".nc")]

    if not all_files:
        print(f"No NetCDF files found in {raw_data_dir}")
        return

    # Define only the variables we need to reduce memory footprint
    required_vars = [
        'time',
        'air_temperature_2m',
        'precipitation_amount',
        'cloud_area_fraction',
        'air_pressure_at_sea_level',
        'wind_speed_10m',
        'wind_speed_of_gust',
        'latitude',
        'longitude'
    ]

    try:
        print(f"Opening {len(all_files)} file(s) with chunking...")
        # Use open_mfdataset to handle multiple files efficiently with dask
        # The 'auto' chunking is a good starting point for performance.
        ds = xr.open_mfdataset(all_files, chunks='auto', combine='by_coords')

        # Select only the required variables - keep variables that are actually in the dataset
        ds = ds[[var for var in required_vars if var in ds.variables]]

        # Rename wind_speed_of_gust to wind_gust for consistency
        if 'wind_speed_of_gust' in ds:
            ds = ds.rename({'wind_speed_of_gust': 'wind_gust'})

        print("Writing to Zarr store...")
        # The computation will happen here, streamed to the Zarr store
        ds.to_zarr(ZARR_STORE_PATH_MET, mode='w', consolidated=True)
        print("Finished writing to Zarr store.")

    except Exception as e:
        print(f"Error processing files: {e}")


def process_met_data(date_str, cycle):
    """
    Processes raw MET data and stores it based on the OUTPUT_FORMAT.
    """
    if OUTPUT_FORMAT == "zarr":
        process_met_data_zarr(date_str, cycle)
    else:
        raise ValueError(f"Unsupported OUTPUT_FORMAT for MET data: {OUTPUT_FORMAT}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process MET data and save to Zarr.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    process_met_data(args.date, args.cycle)