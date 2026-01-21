"""
MEPS 2.5km Data Processor

Processes raw MEPS 2.5km NetCDF data and stores it in Zarr format,
optimized for efficient access and visualization.
"""

import os
import xarray as xr
import argparse
import sys
import numpy as np
import warnings

warnings.filterwarnings("ignore")

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_FORMAT, SCANDINAVIA_BOUNDS

# Variables to extract from MEPS 2.5km data
# Using variable name candidates to handle different naming conventions
MEPS25_VARIABLE_MAPPING = {
    # Output name: list of possible input names
    'air_temperature_2m': ['air_temperature_2m', 'air_temperature_ml', 't2m', 'temperature_2m'],
    'x_wind_10m': ['x_wind_10m', 'x_wind_ml', 'u10', 'eastward_wind_10m'],
    'y_wind_10m': ['y_wind_10m', 'y_wind_ml', 'v10', 'northward_wind_10m'],
    'air_pressure_at_sea_level': ['surface_air_pressure', 'air_pressure_at_sea_level', 'msl', 'ps'],
    'precipitation_amount': ['precipitation_amount_acc', 'precipitation_amount', 
                             'lwe_precipitation_rate', 'precipitation_rate',
                             'integral_of_surface_downward_precipitation_flux_wrt_time'],
    'cloud_area_fraction': ['cloud_area_fraction', 'cloud_area_fraction_in_atmosphere_layer',
                            'total_cloud_cover', 'tcc'],
    'atmosphere_water_vapor_content': ['atmosphere_water_vapor_content',
                                       'integral_of_atmosphere_water_vapor_content_wrt_time',
                                       'lwe_thickness_of_atmosphere_mass_content_of_water_vapor',
                                       'precipitable_water', 'total_precipitable_water'],
}


def find_variable(ds, var_candidates):
    """Find the first matching variable from a list of candidates."""
    for var in var_candidates:
        if var in ds.variables:
            return var
    return None


def process_meps25_data_zarr(date_str, cycle):
    """
    Processes raw MEPS 2.5km data and saves to a Zarr store.
    
    Args:
        date_str (str): Date in YYYYMMDD format
        cycle (str): Forecast cycle (00, 06, 12, 18)
    """
    raw_data_dir = os.path.join('data', 'raw', 'meps25', date_str, cycle)
    zarr_store_path = f"data/processed/meps25_data_{date_str}_{cycle}.zarr"
    os.makedirs(os.path.dirname(zarr_store_path), exist_ok=True)

    if not os.path.exists(raw_data_dir):
        print(f"Raw data directory not found: {raw_data_dir}")
        return False

    # Find all NetCDF files (prefer .nc files over .ncml)
    all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                 if f.endswith(".nc")]
    
    # If no .nc files, try .ncml (though these typically won't work with local file access)
    if not all_files:
        all_files = [os.path.join(raw_data_dir, f) for f in sorted(os.listdir(raw_data_dir))
                     if f.endswith(".ncml")]

    if not all_files:
        print(f"No NetCDF files found in {raw_data_dir}")
        return False

    try:
        print(f"Opening {len(all_files)} file(s)...")
        # Open with chunking for memory efficiency
        ds = xr.open_mfdataset(all_files, chunks='auto', combine='by_coords')

        print(f"Available variables: {list(ds.variables.keys())[:20]}...")

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

        if not lat_var or not lon_var:
            print("Warning: Could not find lat/lon coordinates")
        else:
            print(f"Using coordinates: lat={lat_var}, lon={lon_var}")

        # Subset to Scandinavia domain if coordinates are 1D
        if lat_var and lon_var:
            lats = ds[lat_var].values
            lons = ds[lon_var].values
            
            # Handle curvilinear grids (2D lat/lon arrays)
            if len(lats.shape) == 2:
                # Create mask for domain
                lat_mask = (lats >= SCANDINAVIA_BOUNDS['lat_min']) & (lats <= SCANDINAVIA_BOUNDS['lat_max'])
                lon_mask = (lons >= SCANDINAVIA_BOUNDS['lon_min']) & (lons <= SCANDINAVIA_BOUNDS['lon_max'])
                combined_mask = lat_mask & lon_mask
                
                # Find y and x indices that contain valid data
                y_indices = np.where(combined_mask.any(axis=1))[0]
                x_indices = np.where(combined_mask.any(axis=0))[0]
                
                if len(y_indices) > 0 and len(x_indices) > 0:
                    print(f"Subsetting to Scandinavia domain...")
                    ds = ds.isel(y=slice(y_indices[0], y_indices[-1]+1),
                                x=slice(x_indices[0], x_indices[-1]+1))
            else:
                # 1D coordinates - use sel with slice
                try:
                    ds = ds.sel({
                        lat_var: slice(SCANDINAVIA_BOUNDS['lat_min'], SCANDINAVIA_BOUNDS['lat_max']),
                        lon_var: slice(SCANDINAVIA_BOUNDS['lon_min'], SCANDINAVIA_BOUNDS['lon_max'])
                    })
                    print(f"Subsetted to Scandinavia domain")
                except Exception as e:
                    print(f"Warning: Could not subset to domain: {e}")

        # Extract and rename variables
        extracted_vars = {}
        for output_name, candidates in MEPS25_VARIABLE_MAPPING.items():
            found_var = find_variable(ds, candidates)
            if found_var:
                extracted_vars[output_name] = ds[found_var]
                print(f"  Found {output_name} <- {found_var}")
            else:
                print(f"  Missing: {output_name}")

        if not extracted_vars:
            print("Error: No required variables found in dataset")
            return False

        # Create output dataset
        ds_out = xr.Dataset(extracted_vars)

        # Ensure coordinate variables are included
        coord_vars = ['time', lat_var, lon_var]
        for coord in coord_vars:
            if coord and coord in ds.coords:
                ds_out = ds_out.assign_coords({coord: ds[coord]})

        # Add x, y if present (for curvilinear grids)
        for coord in ['x', 'y']:
            if coord in ds.coords and coord not in ds_out.coords:
                ds_out = ds_out.assign_coords({coord: ds[coord]})

        # Add init_time dimension from forecast_reference_time if available
        if 'forecast_reference_time' in ds:
            init_time_val = ds['forecast_reference_time'].values
            ds_out = ds_out.assign_coords(init_time=init_time_val).expand_dims('init_time')
        else:
            # Create init_time from the date/cycle
            from datetime import datetime
            init_time = datetime.strptime(f"{date_str}{cycle}", '%Y%m%d%H')
            ds_out = ds_out.assign_coords(init_time=np.datetime64(init_time)).expand_dims('init_time')

        # Compute wind speed from components if we have both
        if 'x_wind_10m' in ds_out and 'y_wind_10m' in ds_out:
            ds_out['wind_speed_10m'] = np.sqrt(ds_out['x_wind_10m']**2 + ds_out['y_wind_10m']**2)
            ds_out['wind_speed_10m'].attrs['units'] = 'm/s'
            ds_out['wind_speed_10m'].attrs['long_name'] = 'Wind speed at 10m'
            print("  Computed wind_speed_10m from components")

        print(f"\nWriting to Zarr store: {zarr_store_path}")
        ds_out.to_zarr(zarr_store_path, mode='w', consolidated=True)
        print("Finished writing to Zarr store.")

        return True

    except Exception as e:
        print(f"Error processing files: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_meps25_data(date_str, cycle):
    """
    Processes raw MEPS 2.5km data and stores it based on the OUTPUT_FORMAT.
    """
    if OUTPUT_FORMAT == "zarr":
        return process_meps25_data_zarr(date_str, cycle)
    else:
        raise ValueError(f"Unsupported OUTPUT_FORMAT for MEPS 2.5km data: {OUTPUT_FORMAT}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process MEPS 2.5km data and save to Zarr.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_meps25_data.py --date 20231115 --cycle 06
"""
    )
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    success = process_meps25_data(args.date, args.cycle)
    sys.exit(0 if success else 1)
