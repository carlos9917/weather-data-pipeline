import os
import xarray as xr
import duckdb
import argparse
import sys
import pandas as pd

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
    conn.execute("""
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
    """)

    for file_name in sorted(os.listdir(raw_data_dir)):
        if (file_name.endswith(".grib2") or file_name.startswith("gfs.")) and not file_name.endswith(".idx"):
            file_path = os.path.join(raw_data_dir, file_name)
            print(f"Processing {file_path}")

            try:
                ds_wind = xr.open_dataset(file_path, engine="cfgrib",
                                          backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 100}})
                ds_temp = xr.open_dataset(file_path, engine="cfgrib",
                                          backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2}})
                ds_precip = xr.open_dataset(file_path, engine="cfgrib",
                                             backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface', 'shortName': 'tp'}})
                ds_cloud = xr.open_dataset(file_path, engine="cfgrib",
                                           backend_kwargs={'filter_by_keys': {'typeOfLevel': 'atmosphere', 'shortName': 'tcc'}})
                ds_pwat = xr.open_dataset(file_path, engine="cfgrib",
                                          backend_kwargs={'filter_by_keys': {'typeOfLevel': 'atmosphere', 'shortName': 'pwat'}})
                ds_prmsl = xr.open_dataset(file_path, engine="cfgrib",
                                           backend_kwargs={'filter_by_keys': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}})

                ds = xr.merge([ds_wind, ds_temp, ds_precip, ds_cloud, ds_pwat, ds_prmsl], compat='override')

                # Calculate wind speed and direction
                wind_speed = (ds['u100']**2 + ds['v100']**2)**0.5
                wind_direction = 180 + (180 / 3.14159) * xr.ufuncs.arctan2(ds['u100'], ds['v100'])

                ds['wind_speed'] = wind_speed
                ds['wind_direction'] = wind_direction

                # Convert to pandas DataFrame
                df = ds.to_dataframe().reset_index()
                df = df.rename(columns={
                    'valid_time': 'time',
                    'u100': 'u_wind',
                    'v100': 'v_wind',
                    't2m': 'temperature',
                    'tp': 'precipitation',
                    'tcc': 'cloud_cover',
                    'pwat': 'precipitable_water',
                    'prmsl': 'mean_sea_level_pressure'
                })

                # Select and order columns for insertion
                df = df[['time', 'latitude', 'longitude', 'u_wind', 'v_wind', 'temperature', 'precipitation', 'cloud_cover', 'precipitable_water', 'mean_sea_level_pressure', 'wind_speed', 'wind_direction']]

                # Insert data into DuckDB
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
            # Open datasets for each variable type, filtering by GRIB keys
            ds_wind = xr.open_dataset(file_path, engine="cfgrib",
                                      backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 100}})
            ds_temp = xr.open_dataset(file_path, engine="cfgrib",
                                      backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2}})
            ds_precip = xr.open_dataset(file_path, engine="cfgrib",
                                         backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface', 'shortName': 'tp'}})
            ds_cloud = xr.open_dataset(file_path, engine="cfgrib",
                                       backend_kwargs={'filter_by_keys': {'typeOfLevel': 'atmosphere', 'shortName': 'tcc'}})
            ds_pwat = xr.open_dataset(file_path, engine="cfgrib",
                                      backend_kwargs={'filter_by_keys': {'typeOfLevel': 'atmosphere', 'shortName': 'pwat'}})
            ds_prmsl = xr.open_dataset(file_path, engine="cfgrib",
                                       backend_kwargs={'filter_by_keys': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}})

            # Merge all datasets
            ds = xr.merge([ds_wind, ds_temp, ds_precip, ds_cloud, ds_pwat, ds_prmsl], compat='override')

            # Calculate wind speed and direction
            wind_speed = (ds['u100']**2 + ds['v100']**2)**0.5
            wind_direction = 180 + (180 / 3.14159) * xr.ufuncs.arctan2(ds['u100'], ds['v100'])

            ds['wind_speed'] = wind_speed
            ds['wind_direction'] = wind_direction
            
            # Rename variables for clarity
            ds = ds.rename({
                'u100': 'u_wind',
                'v100': 'v_wind',
                't2m': 'temperature',
                'tp': 'precipitation',
                'tcc': 'cloud_cover',
                'pwat': 'precipitable_water',
                'prmsl': 'mean_sea_level_pressure'
            })
            
            # Append to Zarr store
            # The 'time' dimension is used for appending
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
