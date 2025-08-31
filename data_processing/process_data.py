import os
import xarray as xr
import duckdb
import argparse
import sys
import pandas as pd
import numpy as np

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
    # Define the full schema including all possible columns
    table_schema = """
        CREATE TABLE IF NOT EXISTS gfs_data (
            time TIMESTAMP,
            latitude REAL,
            longitude REAL,
            u_wind REAL,
            v_wind REAL,
            temperature REAL,
            precipitation REAL,
            cloud_cover REAL,
            precipitable_water REAL,
            mean_sea_level_pressure REAL,
            wind_speed REAL,
            wind_direction REAL
        );
    """
    conn.execute(table_schema)
    # Get the list of columns from the schema to ensure DataFrame consistency
    db_columns = [col[0] for col in conn.execute("DESCRIBE gfs_data;").fetchall()]


    for file_name in sorted(os.listdir(raw_data_dir)):
        if (file_name.endswith(".grib2") or file_name.startswith("gfs.")) and not file_name.endswith(".idx"):
            file_path = os.path.join(raw_data_dir, file_name)
            print(f"Processing {file_path}")

            try:
                datasets = []
                variable_filters = {
                    'wind': {'typeOfLevel': 'heightAboveGround', 'level': 100},
                    'temp': {'typeOfLevel': 'heightAboveGround', 'level': 2},
                    'precip': {'typeOfLevel': 'surface', 'shortName': 'tp'},
                    'cloud': {'stepType': 'instant', 'typeOfLevel': 'atmosphere', 'shortName': 'tcc'},
                    'pwat': {'typeOfLevel': 'atmosphere', 'shortName': 'pwat'},
                    'prmsl': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}
                }

                for var, filter_keys in variable_filters.items():
                    try:
                        datasets.append(xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={'filter_by_keys': filter_keys}))
                    except (ValueError, KeyError) as e:
                        print(f"Warning: Could not load variable '{var}' from {file_name}. Reason: {e}")

                if not datasets:
                    print(f"Warning: No processable variables found in {file_name}. Skipping.")
                    continue

                ds = xr.merge(datasets, compat='override')

                # Calculate wind speed and direction if wind variables are present
                if 'u100' in ds and 'v100' in ds:
                    ds['wind_speed'] = (ds['u100']**2 + ds['v100']**2)**0.5
                    ds['wind_direction'] = 180 + (180 / np.pi) * xr.ufuncs.arctan2(ds['u100'], ds['v100'])

                # Dynamically create the rename mapping based on variables present in the dataset
                rename_map = {
                    'valid_time': 'time',
                    'u100': 'u_wind',
                    'v100': 'v_wind',
                    't2m': 'temperature',
                    'tp': 'precipitation',
                    'tcc': 'cloud_cover',
                    'pwat': 'precipitable_water',
                    'prmsl': 'mean_sea_level_pressure'
                }
                actual_rename_map = {k: v for k, v in rename_map.items() if k in ds.variables or k in ds.coords}
                ds = ds.rename(actual_rename_map)

                df = ds.to_dataframe().reset_index()

                # Ensure DataFrame has all columns required by the DB schema, filling missing with NaN
                for col in db_columns:
                    if col not in df.columns:
                        df[col] = np.nan
                
                # Select and order columns for insertion
                df = df[db_columns]

                conn.register('gfs_df', df)
                conn.execute("INSERT INTO gfs_data SELECT * FROM gfs_df")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    conn.close()

def process_gfs_data_zarr(date_str, cycle):
    """
    Processes raw GFS data and appends it to a Zarr store.
    """
    raw_data_dir = os.path.join('data', 'raw', 'gfs', date_str, cycle)
    os.makedirs(os.path.dirname(ZARR_STORE_PATH), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return

    all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                 if (f.endswith(".grib2") or f.startswith("gfs.")) and not f.endswith(".idx")]

    for file_path in all_files:
        print(f"Processing {file_path}")
        try:
            datasets = []
            variable_filters = {
                'wind': {'typeOfLevel': 'heightAboveGround', 'level': 100},
                'temp': {'typeOfLevel': 'heightAboveGround', 'level': 2},
                'precip': {'typeOfLevel': 'surface', 'shortName': 'tp'},
                'cloud': {'stepType': 'instant', 'typeOfLevel': 'atmosphere', 'shortName': 'tcc'},
                'pwat': {'typeOfLevel': 'atmosphere', 'shortName': 'pwat'},
                'prmsl': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}
            }

            for var, filter_keys in variable_filters.items():
                try:
                    datasets.append(xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={'filter_by_keys': filter_keys}))
                except (ValueError, KeyError) as e:
                    print(f"Warning: Could not load variable '{var}' from {file_path}. Reason: {e}")

            if not datasets:
                print(f"Warning: No processable variables found in {file_path}. Skipping.")
                continue
            
            ds = xr.merge(datasets, compat='override')

            if 'u100' in ds and 'v100' in ds:
                ds['wind_speed'] = (ds['u100']**2 + ds['v100']**2)**0.5
                ds['wind_direction'] = 180 + (180 / np.pi) * xr.ufuncs.arctan2(ds['u100'], ds['v100'])
            
            rename_map = {
                'valid_time': 'time',
                'u100': 'u_wind',
                'v100': 'v_wind',
                't2m': 'temperature',
                'tp': 'precipitation',
                'tcc': 'cloud_cover',
                'pwat': 'precipitable_water',
                'prmsl': 'mean_sea_level_pressure'
            }
            actual_rename_map = {k: v for k, v in rename_map.items() if k in ds.variables or k in ds.coords}
            ds = ds.rename(actual_rename_map)
            
            # Promote the 'time' coordinate to a dimension to allow appending
            if 'time' in ds.coords and 'time' not in ds.dims:
                ds = ds.expand_dims('time')

            ds.to_zarr(ZARR_STORE_PATH, mode='a', append_dim="time")

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

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
