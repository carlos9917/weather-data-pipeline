import os
import xarray as xr
import duckdb
import argparse
import sys
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_FORMAT, DATABASE_PATH, ZARR_STORE_PATH

def process_gfs_data_duckdb(date_str, cycle):
    """
    Processes raw GFS data and stores it in a DuckDB database.
    """
    raw_data_dir = os.path.join('data', 'raw', 'gfs', date_str, cycle)
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return

    conn = duckdb.connect(DATABASE_PATH)
    table_schema = """
        CREATE TABLE IF NOT EXISTS gfs_data (
            time TIMESTAMP, latitude REAL, longitude REAL, u_wind REAL, v_wind REAL,
            temperature REAL, precipitation REAL, cloud_cover REAL, precipitable_water REAL,
            mean_sea_level_pressure REAL, wind_speed REAL, wind_direction REAL
        );
    """
    conn.execute(table_schema)
    db_columns = [col[0] for col in conn.execute("DESCRIBE gfs_data;").fetchall()]

    for file_name in sorted(os.listdir(raw_data_dir)):
        if not (file_name.endswith(".grib2") or file_name.startswith("gfs.")) or file_name.endswith(".idx"):
            continue

        file_path = os.path.join(raw_data_dir, file_name)
        print(f"Processing {file_path}")

        try:
            datasets = []
            variable_filters = {
                'wind': {'typeOfLevel': 'heightAboveGround', 'level': 100}, 'temp': {'typeOfLevel': 'heightAboveGround', 'level': 2},
                'precip': {'typeOfLevel': 'surface', 'shortName': 'tp'}, 'cloud': {'stepType': 'instant', 'typeOfLevel': 'atmosphere', 'shortName': 'tcc'},
                'pwat': {'typeOfLevel': 'atmosphere', 'shortName': 'pwat'}, 'prmsl': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}
            }
            for var, filter_keys in variable_filters.items():
                try:
                    datasets.append(xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={'filter_by_keys': filter_keys}))
                except (ValueError, KeyError) as e:
                    print(f"Warning: Could not load variable group '{var}' from {file_name}. Reason: {e}")

            if not datasets:
                print(f"Warning: No processable variables found in {file_name}. Skipping.")
                continue

            ds = xr.merge(datasets, compat='override')

            if 'valid_time' in ds.coords and 'time' not in ds.coords:
                ds = ds.rename({'valid_time': 'time'})

            if 'time' not in ds.coords:
                print(f"FATAL: Could not find 'time' or 'valid_time' coordinate in {file_path}. Skipping file.")
                continue

            if 'u100' in ds and 'v100' in ds:
                ds['wind_speed'] = (ds['u100']**2 + ds['v100']**2)**0.5
                ds['wind_direction'] = 180 + (180 / np.pi) * xr.ufuncs.arctan2(ds['u100'], ds['v100'])

            rename_map = {
                'u100': 'u_wind', 'v100': 'v_wind', 't2m': 'temperature', 'tp': 'precipitation',
                'tcc': 'cloud_cover', 'pwat': 'precipitable_water', 'prmsl': 'mean_sea_level_pressure'
            }
            actual_rename_map = {k: v for k, v in rename_map.items() if k in ds.variables}
            ds = ds.rename(actual_rename_map)

            df = ds.to_dataframe().reset_index()

            for col in db_columns:
                if col not in df.columns:
                    df[col] = np.nan
            df = df[db_columns]

            conn.register('gfs_df', df)
            conn.execute("INSERT INTO gfs_data SELECT * FROM gfs_df")

        except Exception as e:
            print(f"Error processing {file_name}: {e}")

    conn.close()

def process_gfs_data_zarr(date_str, cycle):
    """
    Processes raw GFS data and stores it in a cycle-specific Zarr store.
    """
    raw_data_dir = os.path.join('data', 'raw', 'gfs', date_str, cycle)
    zarr_store_path = os.path.join('data', 'processed', f'gfs_{date_str}_{cycle}.zarr')
    os.makedirs(os.path.dirname(zarr_store_path), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return

    all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                 if (f.endswith(".grib2") or f.startswith("gfs.")) and not f.endswith(".idx")]

    all_datasets_for_cycle = []
    for file_path in all_files:
        print(f"Processing {file_path}")
        try:
            # Load variables individually or in compatible groups to avoid cfgrib merge errors
            datasets_to_merge = []
            variable_filters = [
                {'typeOfLevel': 'heightAboveGround', 'level': 100, 'shortName': ['u', 'v']},
                {'typeOfLevel': 'heightAboveGround', 'level': 10, 'shortName': ['u', 'v']},
                {'typeOfLevel': 'heightAboveGround', 'level': 2, 'shortName': 't'},
                {'typeOfLevel': 'surface', 'shortName': 'sp'},
                {'typeOfLevel': 'surface', 'shortName': 'tp'},
                {'typeOfLevel': 'atmosphere', 'shortName': 'tcc'},
                {'typeOfLevel': 'surface', 'shortName': 'prate'},
                {'typeOfLevel': 'surface', 'shortName': 'gust'},
            ]

            for filters in variable_filters:
                try:
                    ds_var = xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={'filter_by_keys': filters})
                    datasets_to_merge.append(ds_var)
                except Exception as e:
                    # This is expected if a variable is not in the file
                    # print(f"Info: Could not load variable with filter {filters} from {file_path}. Reason: {e}")
                    pass
            
            if not datasets_to_merge:
                print(f"Warning: No processable variables found in {file_path}. Skipping.")
                continue

            # Merge the individually loaded datasets
            ds = xr.merge(datasets_to_merge)
            print(f"Variables found in merged dataset: {list(ds.variables)}")

            # Standardize time coordinate
            if 'valid_time' in ds.coords and 'time' not in ds.coords:
                ds = ds.rename({'valid_time': 'time'})
            
            if 'valid_time' in ds.coords:
                ds = ds.drop_vars('valid_time')

            if 'time' not in ds.coords:
                print(f"Warning: No time coordinate in {file_path}. Skipping.")
                continue

            if 'time' not in ds.dims:
                ds = ds.expand_dims('time')

            # Calculate wind speed
            if 'u10' in ds and 'v10' in ds:
                ds['wind_speed_10m'] = (ds['u10']**2 + ds['v10']**2)**0.5
            if 'u100' in ds and 'v100' in ds:
                ds['wind_speed_100m'] = (ds['u100']**2 + ds['v100']**2)**0.5
            
            # Rename variables
            rename_map = {
                'u10': 'u_wind_10m', 'v10': 'v_wind_10m', 'u100': 'u_wind_100m', 
                'v100': 'v_wind_100m', 't2m': 'temperature', 'tp': 'precipitation',
                'tcc': 'cloud_cover', 'prate': 'precipitation_rate', 'sp': 'surface_pressure',
                'gust': 'wind_gust'
            }
            ds = ds.rename({k: v for k, v in rename_map.items() if k in ds})
            
            all_datasets_for_cycle.append(ds)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    if not all_datasets_for_cycle:
        print("No valid datasets to process for this cycle.")
        return

    # Combine all datasets for the cycle
    print(f"Combining {len(all_datasets_for_cycle)} time steps for cycle {date_str}/{cycle}...")
    cycle_ds = xr.concat(all_datasets_for_cycle, dim='time').sortby('time')

    # Add init_time dimension
    init_time = pd.to_datetime(f"{date_str} {cycle}:00")
    cycle_ds = cycle_ds.assign_coords(init_time=init_time).expand_dims('init_time')

    # Write to cycle-specific Zarr store, overwriting if it exists
    print(f"Creating new Zarr store at {zarr_store_path}")
    cycle_ds.to_zarr(zarr_store_path, mode='w')

def process_gfs_data(date_str, cycle):
    """
    Processes raw GFS data and stores it based on the OUTPUT_FORMAT.
    """
    if OUTPUT_FORMAT == "duckdb":
        process_gfs_data_duckdb(date_str, cycle)
    elif OUTPUT_FORMAT == "zarr":
        process_gfs_data_zarr(date_str, cycle)
    else:
        raise ValueError(f"Unsupported OUTPUT_FORMAT: {OUTPUT_FORMAT}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GFS data and save to DuckDB or Zarr.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    process_gfs_data(args.date, args.cycle)