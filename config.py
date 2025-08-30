# GFS Data Pipeline Configuration

import requests
from datetime import datetime, timedelta

# Base NOMADS URLs
NOMADS_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
NOMADS_GRIB_FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

# GFS cycles and forecast hours
GFS_CYCLES = ["00", "06", "12", "18"]
FORECAST_HOURS = list(range(0, 73, 3))  # 0-72 hours, every 3 hours

# European region bounds (lat/lon)
EUROPE_BOUNDS = {
    'lat_min': 35.0,
    'lat_max': 70.0,
    'lon_min': -15.0,
    'lon_max': 40.0
}

# GFS file naming convention
# Format: gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}
GFS_FILE_TEMPLATE = "gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}"

# Variables needed for wind power density and temperature
GFS_VARIABLES = [
    'UGRD:100 m above ground',  # U-component of wind at 100m
    'VGRD:100 m above ground',  # V-component of wind at 100m
    'TMP:2 m above ground'      # Temperature at 2m
]

# Database settings
DATABASE_PATH = "data/processed/gfs_data.duckdb"

# Logging settings
LOG_LEVEL = "INFO"
LOG_FILE = "logs/pipeline.log"

# Dashboard settings
DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = 8050
DASHBOARD_DEBUG = True

# Data access method: 'direct', 'grib_filter'
ACCESS_METHOD = 'direct'  # Try direct download first

def get_latest_available_date():
    """Get the latest available GFS date from NOMADS"""
    try:
        response = requests.get(NOMADS_BASE_URL, timeout=10)
        if response.status_code == 200:
            content = response.text
            # Look for gfs.YYYYMMDD directories
            import re
            dates = re.findall(r'gfs\.(\d{8})/', content)
            if dates:
                return max(dates)  # Return latest date
    except:
        pass
    
    # Fallback to current date
    return datetime.now().strftime('%Y%m%d')

def get_available_cycles(date_str):
    """Get available cycles for a given date"""
    try:
        url = f"{NOMADS_BASE_URL}/gfs.{date_str}/"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            content = response.text
            # Look for cycle directories (00/, 06/, 12/, 18/)
            import re
            cycles = re.findall(r'(\d{2})/', content)
            return sorted([c for c in cycles if c in GFS_CYCLES])
    except:
        pass
    
    return GFS_CYCLES

def build_gfs_url(date_str, cycle, forecast_hour):
    """Build the complete URL for a GFS file"""
    filename = GFS_FILE_TEMPLATE.format(cycle=cycle, forecast_hour=forecast_hour)
    return f"{NOMADS_BASE_URL}/gfs.{date_str}/{cycle}/atmos/{filename}"

def build_grib_filter_url(date_str, cycle, forecast_hour):
    """Build GRIB filter URL for specific parameters"""
    params = {
        'file': GFS_FILE_TEMPLATE.format(cycle=cycle, forecast_hour=forecast_hour),
        'lev_100_m_above_ground': 'on',
        'lev_2_m_above_ground': 'on',
        'var_TMP': 'on',
        'var_UGRD': 'on',
        'var_VGRD': 'on',
        'subregion': '',
        'leftlon': str(EUROPE_BOUNDS['lon_min']),
        'rightlon': str(EUROPE_BOUNDS['lon_max']),
        'toplat': str(EUROPE_BOUNDS['lat_max']),
        'bottomlat': str(EUROPE_BOUNDS['lat_min']),
        'dir': f'/gfs.{date_str}/{cycle}/atmos'
    }
    
    param_string = '&'.join([f'{k}={v}' for k, v in params.items()])
    return f"{NOMADS_GRIB_FILTER_URL}?{param_string}"
