"""
MEPS 2.5km Data Downloader

Downloads MEPS (MetCoOp Ensemble Prediction System) 2.5km deterministic data
from MET Norway THREDDS server via OPeNDAP.

MEPS data is served as .ncml aggregations which require OPeNDAP access.
This script fetches the data via OPeNDAP with server-side subsetting to the
Scandinavia domain, then saves it locally as NetCDF.

Data source: https://thredds.met.no/thredds/catalog/meps25epsarchive/
"""

import os
import argparse
from datetime import datetime
import sys
import numpy as np

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    MEPS25_OPENDAP_URL,
    MEPS25_CYCLES,
    SCANDINAVIA_BOUNDS
)


def download_meps25_data(date_str, cycle, max_time_steps=None):
    """
    Downloads MEPS 2.5km data for a specific date and cycle via OPeNDAP.

    Args:
        date_str (str): The date in YYYYMMDD format.
        cycle (str): The cycle, e.g., '00', '06', '12', '18'.
        max_time_steps (int): Maximum number of time steps to download (None = all).
    
    Returns:
        bool: True if successful, False otherwise.
    """
    # Import xarray here to avoid import errors if not installed
    try:
        import xarray as xr
    except ImportError:
        print("Error: xarray is required. Install with: pip install xarray netcdf4")
        return False

    # Parse the date string to get year, month, day
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"Error: Invalid date format for {date_str}. Please use YYYYMMDD.")
        return False

    # Validate cycle
    if cycle not in MEPS25_CYCLES:
        print(f"Warning: Cycle '{cycle}' may not be available. Valid cycles: {MEPS25_CYCLES}")

    # Create raw data directory
    raw_data_dir = os.path.join('data', 'raw', 'meps25', date_str, cycle)
    os.makedirs(raw_data_dir, exist_ok=True)

    # Output file path
    output_file = os.path.join(raw_data_dir, f"meps25_{date_str}T{cycle}Z.nc")

    if os.path.exists(output_file):
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        if file_size_mb > 1:  # Only skip if file is larger than 1MB (valid data)
            print(f"File already exists: {output_file} ({file_size_mb:.1f} MB)")
            return True
        else:
            print(f"Removing incomplete file: {output_file}")
            os.remove(output_file)

    # Try different OPeNDAP URL patterns
    url_patterns = [
        f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_sfc_{date_str}T{cycle}Z.ncml",
        f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_2_5km_{date_str}T{cycle}Z.nc",
        f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_ml_{date_str}T{cycle}Z.ncml",
    ]

    ds = None
    successful_url = None

    for url in url_patterns:
        try:
            print(f"Trying OPeNDAP: {url}")
            ds = xr.open_dataset(url)
            successful_url = url
            print(f"Connected successfully!")
            break
        except Exception as e:
            print(f"  -> Failed: {e}")
            continue

    if ds is None:
        print(f"Error: Could not connect to MEPS 2.5km data for {date_str} cycle {cycle}")
        print("Note: Historical data may not be available for all dates.")
        return False

    print(f"Available variables: {len(ds.variables)}")

    # Select only required variables
    required_vars = [
        'air_temperature_2m',
        'x_wind_10m',
        'y_wind_10m',
        'surface_air_pressure',
        'precipitation_amount_acc',
        'lwe_thickness_of_atmosphere_mass_content_of_water_vapor'
    ]

    # Find which required variables are available
    available_required = [v for v in required_vars if v in ds.variables]
    missing_vars = [v for v in required_vars if v not in ds.variables]

    if missing_vars:
        print(f"Warning: Missing variables: {missing_vars}")

    if not available_required:
        print("Error: None of the required variables are available in the dataset")
        return False

    print(f"Selecting {len(available_required)} variables: {', '.join(available_required)}")

    # Get coordinate variables to keep
    coord_vars = []
    for coord in ['time', 'x', 'y', 'latitude', 'longitude', 'lat', 'lon',
                  'projection_lambert', 'height0', 'height1', 'height2', 'height3']:
        if coord in ds.coords or coord in ds.variables:
            coord_vars.append(coord)

    # Select only required variables plus coordinates
    vars_to_keep = available_required + coord_vars
    ds = ds[vars_to_keep]
    print(f"Reduced from 221 to {len(ds.data_vars)} data variables")

    # Find coordinate variables
    lat_var = None
    lon_var = None
    for var in ['latitude', 'lat', 'y']:
        if var in ds.coords or var in ds.variables:
            lat_var = var
            break
    for var in ['longitude', 'lon', 'x']:
        if var in ds.coords or var in ds.variables:
            lon_var = var
            break

    # Subset to Scandinavia domain
    if lat_var and lon_var:
        lats = ds[lat_var].values
        lons = ds[lon_var].values

        print(f"Subsetting to Scandinavia domain (lat {SCANDINAVIA_BOUNDS['lat_min']}-{SCANDINAVIA_BOUNDS['lat_max']}, "
              f"lon {SCANDINAVIA_BOUNDS['lon_min']}-{SCANDINAVIA_BOUNDS['lon_max']})...")

        # Handle curvilinear grids (2D lat/lon arrays)
        if len(lats.shape) == 2:
            lat_mask = (lats >= SCANDINAVIA_BOUNDS['lat_min']) & (lats <= SCANDINAVIA_BOUNDS['lat_max'])
            lon_mask = (lons >= SCANDINAVIA_BOUNDS['lon_min']) & (lons <= SCANDINAVIA_BOUNDS['lon_max'])
            combined_mask = lat_mask & lon_mask

            y_indices = np.where(combined_mask.any(axis=1))[0]
            x_indices = np.where(combined_mask.any(axis=0))[0]

            if len(y_indices) > 0 and len(x_indices) > 0:
                print(f"  Selecting y[{y_indices[0]}:{y_indices[-1]+1}], x[{x_indices[0]}:{x_indices[-1]+1}]")
                ds = ds.isel(y=slice(y_indices[0], y_indices[-1]+1),
                            x=slice(x_indices[0], x_indices[-1]+1))
            else:
                print("  Warning: No data in specified domain, using full domain")
        else:
            # 1D coordinates
            try:
                ds = ds.sel({
                    lat_var: slice(SCANDINAVIA_BOUNDS['lat_min'], SCANDINAVIA_BOUNDS['lat_max']),
                    lon_var: slice(SCANDINAVIA_BOUNDS['lon_min'], SCANDINAVIA_BOUNDS['lon_max'])
                })
                print("  Subset complete")
            except Exception as e:
                print(f"  Warning: Could not subset: {e}")
    else:
        print("Warning: Could not find lat/lon coordinates for subsetting")

    # Limit time steps if requested
    if 'time' in ds.dims:
        total_times = len(ds.time)
        if max_time_steps and total_times > max_time_steps:
            print(f"Limiting to first {max_time_steps} time steps (of {total_times} available)")
            ds = ds.isel(time=slice(0, max_time_steps))
        else:
            print(f"Downloading {total_times} time steps")

    # Download and save
    print(f"Downloading data from server...")
    print(f"Target file: {output_file}")
    print(f"This may take several minutes for large datasets...")
    try:
        import time
        start_time = time.time()
        ds = ds.load()  # This triggers the actual download
        elapsed = time.time() - start_time
        print(f"Download completed in {elapsed:.1f} seconds")

        print(f"Saving to: {output_file}")
        ds.to_netcdf(output_file)

        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"Downloaded: {output_file} ({file_size_mb:.1f} MB)")

        ds.close()
        return True

    except Exception as e:
        print(f"Error downloading data: {e}")
        # Clean up partial file
        if os.path.exists(output_file):
            os.remove(output_file)
        return False


def list_available_files(date_str, cycle):
    """
    List available files for a given date and cycle by printing the catalog URL.
    """
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"Error: Invalid date format for {date_str}. Please use YYYYMMDD.")
        return

    catalog_url = f"https://thredds.met.no/thredds/catalog/meps25epsarchive/{year}/{month}/{day}/catalog.html"
    print(f"Check available files at: {catalog_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download MEPS 2.5km data from MET Norway via OPeNDAP.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python meps25_downloader.py --date 20231115 --cycle 06
  python meps25_downloader.py --date 20231115 --cycle 00 --max-times 24
  python meps25_downloader.py --date 20231115 --cycle 00 --list

Note: MEPS data is accessed via OPeNDAP with server-side subsetting.
      The download is limited to the Scandinavia domain to reduce file size.
"""
    )
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    parser.add_argument("--max-times", type=int, default=None,
                        help="Maximum number of time steps to download (default: all)")
    parser.add_argument("--list", action="store_true", 
                        help="List available files for the date (print catalog URL).")
    args = parser.parse_args()

    if args.list:
        list_available_files(args.date, args.cycle)
    else:
        success = download_meps25_data(args.date, args.cycle, max_time_steps=args.max_times)
        sys.exit(0 if success else 1)
