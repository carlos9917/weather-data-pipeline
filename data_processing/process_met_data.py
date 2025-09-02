import os
import xarray as xr
import argparse
import sys
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_FORMAT

def process_met_data_zarr(date_str, cycle):
    """
    Processes raw MET data and appends it to a Zarr store.
    """
    raw_data_dir = os.path.join('data', 'raw', 'met', date_str, cycle)
    ZARR_STORE_PATH_MET = "data/processed/met_data.zarr"
    os.makedirs(os.path.dirname(ZARR_STORE_PATH_MET), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return

    all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                 if f.endswith(".nc")]

    if not all_files:
        print(f"No NetCDF files found in {raw_data_dir}")
        return

    all_datasets = []
    for file_path in all_files:
        print(f"Processing {file_path}")
        try:
            ds = xr.open_dataset(file_path)
            all_datasets.append(ds)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    if not all_datasets:
        print("No valid datasets to process.")
        return

    try:
        print(f"Combining {len(all_datasets)} datasets...")
        combined_ds = xr.concat(all_datasets, dim='time')
        combined_ds = combined_ds.sortby('time')

        # Rename wind_speed_of_gust to wind_gust for consistency
        if 'wind_speed_of_gust' in combined_ds:
            combined_ds = combined_ds.rename({'wind_speed_of_gust': 'wind_gust'})


        zarr_exists = os.path.exists(ZARR_STORE_PATH_MET)
        if not zarr_exists:
            print(f"Creating new Zarr store at {ZARR_STORE_PATH_MET}")
            combined_ds.to_zarr(ZARR_STORE_PATH_MET, mode='w')
        else:
            print(f"Appending to existing Zarr store")
            try:
                existing_ds = xr.open_zarr(ZARR_STORE_PATH_MET)
                if 'time' in existing_ds.dims:
                    combined_ds.to_zarr(ZARR_STORE_PATH_MET, mode='a', append_dim="time")
                else:
                    print(f"Warning: Existing Zarr store doesn't have time dimension. Creating new store.")
                    combined_ds.to_zarr(ZARR_STORE_PATH_MET, mode='w')
            except Exception as zarr_error:
                print(f"Error accessing existing Zarr store: {zarr_error}")
                print(f"Creating new Zarr store.")
                combined_ds.to_zarr(ZARR_STORE_PATH_MET, mode='w')

    except Exception as e:
        print(f"Error combining or writing datasets: {e}")

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