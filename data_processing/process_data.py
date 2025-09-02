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
    Processes raw GFS data and appends it to a Zarr store.
    """
    raw_data_dir = os.path.join('data', 'raw', 'gfs', date_str, cycle)
    os.makedirs(os.path.dirname(ZARR_STORE_PATH), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return

    all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                 if (f.endswith(".grib2") or f.startswith("gfs.")) and not f.endswith(".idx")]

    # Collect all datasets first, then process together
    all_datasets = []
    
    for file_path in all_files:
        print(f"Processing {file_path}")
        try:
            datasets = []
            variable_filters = {
                'wind100': {'typeOfLevel': 'heightAboveGround', 'level': 100}, 
                'wind10': {'typeOfLevel': 'heightAboveGround', 'level': 10}, 
                'temp': {'typeOfLevel': 'heightAboveGround', 'level': 2},
                'sp': {'typeOfLevel': 'surface', 'shortName': 'sp'},
                'precip': {'typeOfLevel': 'surface', 'shortName': 'tp'}, 
                'cloud': {'stepType': 'instant', 'typeOfLevel': 'atmosphere', 'shortName': 'tcc'},
                'prate': {'stepType': 'instant', 'typeOfLevel': 'surface', 'shortName': 'prate'},
                'gust': {'typeOfLevel': 'surface', 'shortName': 'gust'},
            }
            
            for var, filter_keys in variable_filters.items():
                try:
                    ds_var = xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={'filter_by_keys': filter_keys})
                    datasets.append(ds_var)
                except (ValueError, KeyError) as e:
                    print(f"Warning: Could not load variable group '{var}' from {file_path}. Reason: {e}")

            if not datasets:
                print(f"Warning: No processable variables found in {file_path}. Skipping.")
                continue
            
            # Align all datasets to the same time coordinate before merging
            reference_time = None
            aligned_datasets = []
            
            for ds in datasets:
                # Standardize time coordinate name
                if 'valid_time' in ds.coords and 'time' not in ds.coords:
                    ds = ds.rename({'valid_time': 'time'})
                
                if 'time' not in ds.coords:
                    print(f"Warning: No time coordinate found in dataset from {file_path}")
                    continue
                
                # Ensure time is expanded as a dimension if it's just a scalar coordinate
                if 'time' in ds.coords and 'time' not in ds.dims:
                    ds = ds.expand_dims('time')
                    
                # Set reference time from first dataset
                if reference_time is None:
                    reference_time = ds.coords['time']
                
                # Only try to interpolate if both datasets have time as a dimension
                # and if the time coordinates don't match
                try:
                    if ('time' in ds.dims and 'time' in reference_time.dims and 
                        not ds.coords['time'].equals(reference_time)):
                        print(f"Warning: Time coordinate mismatch in {file_path}, interpolating to reference time")
                        ds = ds.interp(time=reference_time, method='nearest')
                    elif 'time' not in ds.dims or 'time' not in reference_time.dims:
                        # If either doesn't have time as dimension, just ensure they're both expanded
                        pass
                except Exception as interp_error:
                    print(f"Warning: Could not interpolate time for {file_path}: {interp_error}")
                
                aligned_datasets.append(ds)
            
            if not aligned_datasets:
                print(f"Warning: No datasets with valid time coordinates in {file_path}. Skipping.")
                continue
                
            # Merge aligned datasets
            ds = xr.merge(aligned_datasets, compat='override')

            if 'time' not in ds.coords:
                print(f"FATAL: Could not find 'time' coordinate after merging in {file_path}. Skipping file.")
                continue

            # Calculate wind speed and direction if wind components are present
            if 'u10' in ds and 'v10' in ds:
                ds['wind_speed_10m'] = (ds['u10']**2 + ds['v10']**2)**0.5
                ds['wind_direction_10m'] = 180 + (180 / np.pi) * xr.ufuncs.arctan2(ds['u10'], ds['v10'])
            if 'u100' in ds and 'v100' in ds:
                ds['wind_speed_100m'] = (ds['u100']**2 + ds['v100']**2)**0.5
                ds['wind_direction_100m'] = 180 + (180 / np.pi) * xr.ufuncs.arctan2(ds['u100'], ds['v100'])
            
            # Rename variables to standard names
            rename_map = {
                'u10': 'u_wind_10m', 'v10': 'v_wind_10m',
                'u100': 'u_wind_100m', 'v100': 'v_wind_100m', 't2m': 'temperature', 'tp': 'precipitation',
                'tcc': 'cloud_cover', 'prate': 'precipitation_rate', 'sp': 'surface_pressure',
                'gust': 'wind_gust'
            }
            actual_rename_map = {k: v for k, v in rename_map.items() if k in ds.variables}
            ds = ds.rename(actual_rename_map)
            
            # Ensure time is a dimension after all processing
            if 'time' in ds.coords and 'time' not in ds.dims:
                ds = ds.expand_dims('time')
            
            # Add init_time coordinate
            init_time = pd.to_datetime(f"{date_str} {cycle}:00")
            ds = ds.assign_coords(init_time=init_time)
            ds = ds.expand_dims('init_time')

            all_datasets.append(ds)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    # Now combine all datasets and write to Zarr
    if all_datasets:
        try:
            print(f"Combining {len(all_datasets)} datasets...")
            
            # Concatenate all datasets along time dimension
            combined_ds = xr.concat(all_datasets, dim='time')
            
            # Sort by time to ensure proper ordering
            combined_ds = combined_ds.sortby('time')
            
            # Check if Zarr store exists
            zarr_exists = os.path.exists(ZARR_STORE_PATH)
            
            if not zarr_exists:
                print(f"Creating new Zarr store at {ZARR_STORE_PATH}")
                combined_ds.to_zarr(ZARR_STORE_PATH, mode='w')
            else:
                print(f"Appending to existing Zarr store")
                try:
                    existing_ds = xr.open_zarr(ZARR_STORE_PATH)
                    if 'time' in existing_ds.dims:
                        combined_ds.to_zarr(ZARR_STORE_PATH, mode='a', append_dim="time")
                    else:
                        print(f"Warning: Existing Zarr store doesn't have time dimension. Creating new store.")
                        combined_ds.to_zarr(ZARR_STORE_PATH, mode='w')
                except Exception as zarr_error:
                    print(f"Error accessing existing Zarr store: {zarr_error}")
                    print(f"Creating new Zarr store.")
                    combined_ds.to_zarr(ZARR_STORE_PATH, mode='w')
                    
        except Exception as e:
            print(f"Error combining or writing datasets: {e}")
    else:
        print("No valid datasets to process.")

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
