# GFS data extractor
"""
GFS Data Extractor Module 
Handles downloading and processing of NOAA GFS forecast data using correct NOMADS URLs
"""

import os
import duckdb
import pandas as pd
import numpy as np
import xarray as xr
import requests
from datetime import datetime, timedelta
from loguru import logger
import dask.array as da
from pathlib import Path
import sys
sys.path.append('config')
from config import *

class GFSDataExtractor:
    def __init__(self):
        self.setup_directories()
        self.setup_logging()
        self.setup_database()
        
    def setup_directories(self):
        """Ensure all required directories exist"""
        directories = ['data/raw', 'data/processed', 'logs', 'config', 'src']
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
    def setup_logging(self):
        """Setup logging with loguru"""
        logger.add(LOG_FILE, rotation="10 MB", level=LOG_LEVEL)
        logger.info("GFS Data Extractor initialized")
        
    def setup_database(self):
        """Initialize DuckDB database"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            
            # Create tables
            conn.execute("CREATE SEQUENCE IF NOT EXISTS gfs_forecasts_id_seq;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gfs_forecasts (
                    id INTEGER PRIMARY KEY DEFAULT nextval('gfs_forecasts_id_seq'),
                    forecast_date TEXT,
                    cycle TEXT,
                    forecast_hour INTEGER,
                    lat DOUBLE,
                    lon DOUBLE,
                    u_wind_10m DOUBLE,
                    v_wind_10m DOUBLE,
                    wind_direction_10m DOUBLE,
                    u_wind_100m DOUBLE,
                    v_wind_100m DOUBLE,
                    wind_direction_100m DOUBLE,
                    temp_2m DOUBLE,
                    total_cloud_cover DOUBLE,
                    precipitation_rate DOUBLE,
                    surface_pressure DOUBLE,
                    wind_power_density DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("CREATE SEQUENCE IF NOT EXISTS country_rankings_id_seq;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS country_rankings (
                    id INTEGER PRIMARY KEY DEFAULT nextval('country_rankings_id_seq'),
                    forecast_date TEXT,
                    cycle TEXT,
                    country TEXT,
                    avg_wind_power_density DOUBLE,
                    rank INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.close()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            
    def check_file_availability(self, date_str, cycle, forecast_hour):
        """Check if a specific GFS file is available"""
        url = build_gfs_url(date_str, cycle, forecast_hour)
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except:
            return False
            
    def download_gfs_file(self, date_str, cycle, forecast_hour):
        """Download GFS file using specified method"""
        return self.download_direct(date_str, cycle, forecast_hour)
            
    def download_direct(self, date_str, cycle, forecast_hour):
        """Download GFS file directly from NOMADS"""
        url = build_gfs_url(date_str, cycle, forecast_hour)
        logger.info(f"Downloading: {url}")
        
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            # Save to temporary file
            temp_file = f"data/raw/gfs_{date_str}_{cycle}_{forecast_hour:03d}.grb2"
            with open(temp_file, 'wb') as f:
                f.write(response.content)
                
            #logger.info(f"Downloaded {len(response.content)} bytes to {temp_file}")
            logger.info(f"Downloaded {len(response.content) / (1024*1024):.2f} MB to{temp_file}")
            return temp_file
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return None
            
    def process_grib_file(self, file_path, date_str, cycle, forecast_hour):
        """Process a single GRIB2 file"""
        try:
            logger.info(f"Processing GRIB file: {file_path}")

            # Define the variables we need and the filters to extract them.
            # The key is the name we expect cfgrib to assign to the variable.
            target_variables = {
                't2m':  {'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2, 'shortName': '2t'}},
                'u10':  {'filter_by_keys': {'typeOfLevel': 'heightAboveGround','level': 10, 'shortName': '10u'}},
                'v10':  {'filter_by_keys': {'typeOfLevel': 'heightAboveGround','level': 10, 'shortName': '10v'}},
                'u100': {'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 100, 'shortName': '100u'}},
                'v100': {'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 100, 'shortName': '100v'}},
                'sp':   {'filter_by_keys': {'typeOfLevel': 'surface', 'shortName': 'sp'}},
                'tcc':  {'filter_by_keys': {'typeOfLevel': 'atmosphere', 'shortName': 'tcc'}},
                'prate':{'filter_by_keys': {'typeOfLevel': 'surface', 'shortName': 'prate'}},
                'tke':  {'filter_by_keys': {'typeOfLevel': 'planetaryBoundaryLayer', 'shortName': 'tke'}},
            }

            ds_list = []
            for var_name, backend_kwargs in target_variables.items():
                try:
                    #ds_single = xr.open_dataset(file_path, engine='cfgrib', backend_kwargs=backend_kwargs)
                    ds_single = xr.open_dataset(
                        file_path,
                        engine='cfgrib',
                        backend_kwargs=backend_kwargs,
                        decode_timedelta=True
                    )

                    ds_list.append(ds_single)
                except Exception as e:
                    logger.warning(f"Could not extract variable for filter {backend_kwargs}. It might be missing. Error: {e}")

            if len(ds_list) < 4:
                logger.error(f"Failed to extract all required variables from {file_path}. Found {len(ds_list)} out of 4.")
                return None

            # Merge the datasets, overriding the conflicting coordinates like 'heightAboveGround'
            ds = xr.merge(ds_list, compat='override')

            # Subset for European region
            lon_min_converted = EUROPE_BOUNDS['lon_min'] % 360
            lon_max_converted = EUROPE_BOUNDS['lon_max'] % 360
            
            if lon_min_converted > lon_max_converted:
                 ds_subset = ds.where((ds.longitude >= lon_min_converted) | (ds.longitude <= lon_max_converted), drop=True)
            else:
                ds_subset = ds.sel(longitude=slice(lon_min_converted, lon_max_converted))

            ds_subset = ds_subset.sel(latitude=slice(EUROPE_BOUNDS['lat_max'], EUROPE_BOUNDS['lat_min']))

            # Create a DataFrame from the xarray Dataset
            df = ds_subset.to_dataframe().reset_index()

            # Calculate wind speed using correct variable names from cfgrib
            wind_speed_10m = np.sqrt(df['u10']**2 + df['v10']**2)
            wind_speed_100m = np.sqrt(df['u100']**2 + df['v100']**2)

            # Calculate wind direction
            wind_direction_10m = (270 - np.rad2deg(np.arctan2(df['v10'], df['u10']))) % 360
            wind_direction_100m = (270 - np.rad2deg(np.arctan2(df['v100'], df['u100']))) % 360

            # Calculate air density (rho) using the ideal gas law: rho = P / (R * T)
            R_specific = 287.058
            air_density = df['sp'] / (R_specific * df['t2m'])

            # Calculate wind power density (W/m^2)
            wind_power_density = 0.5 * air_density * (wind_speed_100m**3)

            # Prepare final DataFrame
            df_final = pd.DataFrame({
                'forecast_date': date_str,
                'cycle': cycle,
                'forecast_hour': forecast_hour,
                'lat': df['latitude'],
                'lon': df['longitude'],
                'u_wind_10m': df['u10'],
                'v_wind_10m': df['v10'],
                'wind_direction_10m': wind_direction_10m,
                'u_wind_100m': df['u100'],
                'v_wind_100m': df['v100'],
                'wind_direction_100m': wind_direction_100m,
                'temp_2m': df['t2m'],
                'total_cloud_cover': df['tcc'],
                'precipitation_rate': df['prate'],
                'surface_pressure': df['sp'],
                'wind_power_density': wind_power_density,
                'tke_pbl': df.get('tke', np.nan)
            })

            # Convert longitude back to -180 to 180 for easier use in GIS
            df_final['lon'] = df_final['lon'].apply(lambda x: x - 360 if x > 180 else x)
            df_final = df_final.dropna()

            logger.info(f"Processed {len(df_final)} data points from {file_path}")
            return df_final

        except Exception as e:
            logger.error(f"Failed to process GRIB file {file_path}: {e}")
            return None
        finally:
            # Clean up temporary file
            if os.path.exists(file_path):
                os.remove(file_path)
                
    def save_to_database(self, df):
        """Save processed data to DuckDB database"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            
            # Select relevant columns for database
            db_columns = [
                'forecast_date', 'cycle', 'forecast_hour', 'lat', 'lon',
                'u_wind_10m', 'v_wind_10m', 'wind_direction_10m',
                'u_wind_100m', 'v_wind_100m', 'wind_direction_100m',
                'temp_2m', 'total_cloud_cover', 'precipitation_rate', 'surface_pressure',
                'wind_power_density'
            ]
            
            # Only keep columns that exist in the dataframe
            available_columns = [col for col in db_columns if col in df.columns]
            df_db = df[available_columns]
            
            # Insert data using an explicit INSERT statement
            columns_str = ", ".join(available_columns)
            conn.execute(f"INSERT INTO gfs_forecasts ({columns_str}) SELECT {columns_str} FROM df_db")
            
            conn.close()
            logger.info(f"Saved {len(df_db)} records to database")
            
        except Exception as e:
            logger.error(f"Failed to save data to database: {e}")
            
    def run_extraction(self, date_str=None, cycle=None):
        """Run complete data extraction pipeline"""
        if date_str is None:
            date_str = get_latest_available_date()
            logger.info(f"Using latest available date: {date_str}")
        
        if cycle is None:
            available_cycles = get_available_cycles(date_str)
            cycles_to_process = available_cycles
        else:
            cycles_to_process = [cycle]
        
        logger.info(f"Processing cycles: {cycles_to_process} for date {date_str}")
        
        for cycle in cycles_to_process:
            logger.info(f"Processing cycle {cycle} for date {date_str}")
            
            # Use Dask to process forecast hours in parallel
            from dask import delayed, compute
            
            lazy_results = []
            for forecast_hour in FORECAST_HOURS:
                # Wrap the processing of each forecast hour in a Dask delayed object
                lazy_result = delayed(self.process_single_forecast_hour)(date_str, cycle, forecast_hour)
                lazy_results.append(lazy_result)
            
            # Compute all delayed results in parallel
            results = compute(*lazy_results)
            
            # Filter out None results (from failed downloads/processing)
            cycle_data = [res for res in results if res is not None]
            
            # Combine all forecast hours for this cycle
            if cycle_data:
                combined_df = pd.concat(cycle_data, ignore_index=True)
                
                # Save to database
                self.save_to_database(combined_df)
                
                logger.info(f"Completed processing for {date_str} cycle {cycle}: {len(combined_df)} total records")
            else:
                logger.error(f"No data processed for {date_str} cycle {cycle}")

    def process_single_forecast_hour(self, date_str, cycle, forecast_hour):
        """Process a single forecast hour"""
        logger.info(f"Processing forecast hour {forecast_hour}")
        
        # Try direct download first, then GRIB filter
        file_path = None
        file_path = self.download_gfs_file(date_str, cycle, forecast_hour)

        if not file_path:
            logger.warning(f"Could not download data for {date_str} cycle {cycle} hour {forecast_hour}")
            return None
        
        # Process the downloaded file
        df = self.process_grib_file(file_path, date_str, cycle, forecast_hour)
        if df is not None:
            return df
        else:
            logger.warning(f"Could not process data for {date_str} cycle {cycle} hour {forecast_hour}")
            return None

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='GFS Data Extractor')
    parser.add_argument('--date', help='Date for extraction (YYYYMMDD)')
    parser.add_argument('--cycle', choices=['00', '06', '12', '18'], 
                       help='GFS cycle for extraction')
    
    args = parser.parse_args()
  
