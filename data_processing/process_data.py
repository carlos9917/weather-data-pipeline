
import os
import xarray as xr
import duckdb
import argparse
import sys
import pandas as pd

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATABASE_PATH, GFS_VARIABLES

def process_gfs_data(date_str, cycle):
    """
    Processes raw GFS data and stores it in a DuckDB database.

    Args:
        date_str (str): The date in YYYYMMDD format.
        cycle (str): The cycle ('00', '06', '12', '18').
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
            wind_speed REAL,
            wind_direction REAL,
            precipitation REAL,
            cloud_cover REAL
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
                ds = xr.merge([ds_wind, ds_temp, ds_precip, ds_cloud], compat='override')

                # Extract variables
                u_wind = ds['u100']
                v_wind = ds['v100']
                temp = ds['t2m']
                precip = ds['tp']
                cloud = ds['tcc']

                # Calculate wind speed and direction
                wind_speed = (u_wind**2 + v_wind**2)**0.5
                wind_direction = 180 + (180 / 3.14159) * xr.ufuncs.arctan2(u_wind, v_wind)

                # Convert to pandas DataFrame
                df = ds.to_dataframe().reset_index()
                df = df.drop(columns=['heightAboveGround', 'time', 'step', 'atmosphere', 'surface'])
                df['u_wind'] = u_wind.values.flatten()
                df['v_wind'] = v_wind.values.flatten()
                df['temperature'] = temp.values.flatten()
                df['wind_speed'] = wind_speed.values.flatten()
                df['wind_direction'] = wind_direction.values.flatten()
                df['precipitation'] = precip.values.flatten()
                df['cloud_cover'] = cloud.values.flatten()
                df = df.rename(columns={'valid_time': 'time'})

                # Select and order columns for insertion
                df = df[['time', 'latitude', 'longitude', 'u_wind', 'v_wind', 'temperature', 'wind_speed', 'wind_direction', 'precipitation', 'cloud_cover']]

                # Insert data into DuckDB
                conn.register('gfs_df', df)
                conn.execute("INSERT INTO gfs_data SELECT * FROM gfs_df")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GFS data.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    process_gfs_data(args.date, args.cycle)
