"""
Enhanced configuration file for weather data pipeline supporting both GFS and MEPS datasets
"""

import os

# Data storage configuration
OUTPUT_FORMAT = "duckdb"  # Options: "duckdb", "zarr"
DATABASE_PATH = "data/processed/weather_data.db"
ZARR_STORE_PATH = "data/processed/weather_data.zarr"
MEPS_ZARR_STORE_PATH = "data/processed/meps_weather_data.zarr"

# Geographic bounds
EUROPE_BOUNDS = {
    'lon_min': -10,
    'lon_max': 30,
    'lat_min': 35,
    'lat_max': 70
}

NORDIC_BOUNDS = {
    'lon_min': -20,
    'lon_max': 80,
    'lat_min': 51,
    'lat_max': 88
}

# GFS Configuration
GFS_BASE_URL = "https://nomads.ncep.noaa.gov/dods"
GFS_VARIABLES = [
    'ugrdprs', 'vgrdprs',  # Wind components at pressure levels
    'tmp2m',               # 2-meter temperature
    'pratesfc',           # Precipitation rate
    'tcdc',               # Total cloud cover
    'pwat',               # Precipitable water
    'prmslmsl'            # Pressure reduced to mean sea level
]

# MEPS (MET Nordic) Configuration
MET_NORDIC_BASE_URL = "https://thredds.met.no/thredds/fileServer/meps25files"
MET_NORDIC_FILE_TEMPLATE = "met_forecast_1_0km_nordic_{date}T{cycle}Z.nc"

# MEPS variables from the NetCDF file structure
MEPS_VARIABLES = {
    # Core meteorological variables
    'air_temperature_2m': {
        'standard_name': 'air_temperature',
        'units': 'K',
        'description': 'Air temperature at 2 meters above ground'
    },
    'precipitation_amount': {
        'standard_name': 'precipitation_amount', 
        'units': 'kg/m^2',
        'description': 'Accumulated precipitation'
    },
    'wind_direction_10m': {
        'standard_name': 'wind_from_direction',
        'units': 'degree',
        'description': 'Wind direction at 10 meters above ground'
    },
    'wind_speed_10m': {
        'standard_name': 'wind_speed',
        'units': 'm/s', 
        'description': 'Wind speed at 10 meters above ground'
    },
    'wind_speed_of_gust': {
        'standard_name': 'wind_speed_of_gust',
        'units': 'm/s',
        'description': 'Wind gust speed'
    },
    'cloud_area_fraction': {
        'standard_name': 'cloud_area_fraction',
        'units': '1',
        'description': 'Cloud area fraction'
    },
    'air_pressure_at_sea_level': {
        'standard_name': 'air_pressure_at_sea_level',
        'units': 'Pa',
        'description': 'Air pressure at mean sea level'
    },
    'relative_humidity_2m': {
        'standard_name': 'relative_humidity',
        'units': '1',
        'description': 'Relative humidity at 2 meters above ground'
    },
    'probability_of_snow_phase': {
        'standard_name': 'snowfall_amount', 
        'units': '1',
        'description': 'Probability of snow phase precipitation'
    },
    
    # Radiation variables
    'integral_of_surface_downwelling_longwave_flux_in_air_wrt_time': {
        'standard_name': 'integral_of_surface_downwelling_longwave_flux_in_air_wrt_time',
        'units': 'J/m^2',
        'description': 'Integrated downwelling longwave radiation'
    },
    'integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time': {
        'standard_name': 'integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time',
        'units': 'W s/m^2',
        'description': 'Integrated downwelling shortwave radiation'
    },
    
    # Surface characteristics
    'altitude': {
        'standard_name': 'surface_altitude',
        'units': 'm',
        'description': 'Surface altitude above sea level'
    },
    'land_area_fraction': {
        'standard_name': 'land_area_fraction',
        'units': '1',
        'description': 'Fraction of grid cell covered by land'
    }
}

# Visualization settings
PLOT_SETTINGS = {
    'figure_size': (12, 10),
    'dpi': 150,
    'colormap_default': 'viridis',
    'contour_levels': 20,
    'scatter_size': 1,
    'barb_length': 6,
    'barb_skip_factor': 10  # Show every 10th wind barb
}

# Dash app settings
DASH_CONFIG = {
    'host': '0.0.0.0',
    'port': 8050,
    'debug': True,
    'assets_folder': 'assets',
    'suppress_callback_exceptions': True
}

# Data processing settings
PROCESSING_CONFIG = {
    'meps_spatial_subset_factor': 10,  # Take every 10th point to reduce data size
    'gfs_spatial_resolution': 0.25,    # GFS resolution in degrees
    'meps_spatial_resolution': 0.01,   # MEPS ~1km resolution
    'max_memory_usage_gb': 8,          # Maximum memory usage for processing
    'chunk_size': 1000000             # Number of points to process at once
}

# Error handling and logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'logs/weather_pipeline.log'
}

# Create necessary directories
REQUIRED_DIRS = [
    'data/raw/gfs',
    'data/raw/met', 
    'data/processed',
    'visualization/plots',
    'logs',
    'assets'
]

def create_directories():
    """Create required directories if they don't exist"""
    for directory in REQUIRED_DIRS:
        os.makedirs(directory, exist_ok=True)

def get_variable_info(dataset_type, variable_name):
    """
    Get information about a specific variable
    
    Args:
        dataset_type (str): 'gfs' or 'meps'
        variable_name (str): Name of the variable
        
    Returns:
        dict: Variable information including units, description, etc.
    """
    if dataset_type.lower() == 'meps':
        return MEPS_VARIABLES.get(variable_name, {})
    elif dataset_type.lower() == 'gfs':
        # GFS variable mapping (from your existing code)
        gfs_mapping = {
            'u_wind': {'units': 'm/s', 'description': 'U-component of wind'},
            'v_wind': {'units': 'm/s', 'description': 'V-component of wind'},
            'temperature': {'units': 'K', 'description': '2-meter air temperature'},
            'precipitation': {'units': 'kg/m^2', 'description': 'Total precipitation'},
            'cloud_cover': {'units': '1', 'description': 'Total cloud cover fraction'},
            'precipitable_water': {'units': 'kg/m^2', 'description': 'Precipitable water'},
            'mean_sea_level_pressure': {'units': 'Pa', 'description': 'Mean sea level pressure'},
            'wind_speed': {'units': 'm/s', 'description': 'Wind speed'},
            'wind_direction': {'units': 'degrees', 'description': 'Wind direction'}
        }
        return gfs_mapping.get(variable_name, {})
    
    return {}

def get_dataset_bounds(dataset_type):
    """
    Get geographic bounds for a dataset type
    
    Args:
        dataset_type (str): 'gfs' or 'meps'
        
    Returns:
        dict: Geographic bounds
    """
    if dataset_type.lower() == 'meps':
        return NORDIC_BOUNDS
    elif dataset_type.lower() == 'gfs':
        return EUROPE_BOUNDS
    
    return EUROPE_BOUNDS

# Environment-specific overrides
if os.environ.get('WEATHER_ENV') == 'production':
    DASH_CONFIG['debug'] = False
    LOGGING_CONFIG['level'] = 'WARNING'

if os.environ.get('WEATHER_OUTPUT_FORMAT'):
    OUTPUT_FORMAT = os.environ.get('WEATHER_OUTPUT_FORMAT')

if os.environ.get('WEATHER_DB_PATH'):
    DATABASE_PATH = os.environ.get('WEATHER_DB_PATH')

if os.environ.get('WEATHER_ZARR_PATH'):
    ZARR_STORE_PATH = os.environ.get('WEATHER_ZARR_PATH')

# Initialize directories when module is imported
create_directories()
