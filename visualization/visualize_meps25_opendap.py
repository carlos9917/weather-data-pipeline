"""
MEPS 2.5km Standalone OPeNDAP Visualizer

Fetches MEPS 2.5km data via OPeNDAP with server-side subsetting and creates
individual map plots in the create_met_visualizations.py style.

This is a standalone script that doesn't require the full pipeline.
"""

import os
import xarray as xr
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import argparse
import sys
import numpy as np
import pandas as pd
import glob
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SCANDINAVIA_BOUNDS, MEPS25_OPENDAP_URL, TURBINE_DATA_PATH

# Variable name candidates for each parameter
VARIABLE_CANDIDATES = {
    'air_temperature_2m': ['air_temperature_2m', 'air_temperature_ml', 't2m', 'temperature_2m'],
    'x_wind_10m': ['x_wind_10m', 'x_wind_ml', 'u10', 'eastward_wind_10m'],
    'y_wind_10m': ['y_wind_10m', 'y_wind_ml', 'v10', 'northward_wind_10m'],
    'air_pressure_at_sea_level': ['surface_air_pressure', 'air_pressure_at_sea_level', 'msl', 'ps'],
    'precipitation_amount': ['precipitation_amount_acc', 'precipitation_amount', 
                             'lwe_precipitation_rate', 'precipitation_rate'],
    'cloud_area_fraction': ['cloud_area_fraction', 'cloud_area_fraction_in_atmosphere_layer',
                            'total_cloud_cover', 'tcc'],
    'atmosphere_water_vapor_content': ['atmosphere_water_vapor_content',
                                       'precipitable_water', 'total_precipitable_water'],
}

# Plotting configurations
PLOT_CONFIG = {
    'air_temperature_2m': {
        'title': 'Temperature at 2m',
        'unit': '°C',
        'cmap': 'coolwarm',
        'levels': np.linspace(-25, 25, 21),
        'convert_to_celsius': True
    },
    'precipitation_amount': {
        'title': 'Precipitation Amount',
        'unit': 'kg/m²',
        'cmap': 'Blues',
        'levels': [0, 0.1, 0.5, 1, 2, 5, 10, 20],
        'norm': mcolors.BoundaryNorm(boundaries=[0, 0.1, 0.5, 1, 2, 5, 10, 20], ncolors=256)
    },
    'cloud_area_fraction': {
        'title': 'Total Cloud Cover',
        'unit': '%',
        'cmap': 'Greys_r',
        'levels': np.linspace(0, 100, 11),
        'multiply_by': 100
    },
    'air_pressure_at_sea_level': {
        'title': 'Sea Level Pressure',
        'unit': 'hPa',
        'cmap': 'viridis',
        'levels': np.linspace(980, 1050, 15),
        'convert_to_hpa': True
    },
    'wind_speed_10m': {
        'title': 'Wind Speed at 10m',
        'unit': 'm/s',
        'cmap': 'viridis',
        'levels': np.linspace(0, 25, 11)
    },
    'atmosphere_water_vapor_content': {
        'title': 'Precipitable Water Vapor',
        'unit': 'kg/m²',
        'cmap': 'YlGnBu',
        'levels': np.linspace(0, 50, 11)
    }
}


def get_turbine_locations():
    """Read turbine locations from CSV files."""
    csv_files = glob.glob(os.path.join(TURBINE_DATA_PATH, '*_processed.csv.gz'))
    
    all_lats = []
    all_lons = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, compression='gzip', nrows=100)
            if 'lat' in df.columns and 'lon' in df.columns:
                unique_turbines = df[['lat', 'lon']].drop_duplicates()
                all_lats.extend(unique_turbines['lat'].tolist())
                all_lons.extend(unique_turbines['lon'].tolist())
        except Exception:
            continue
    
    # Remove duplicates
    if all_lats and all_lons:
        coords = list(set(zip(all_lats, all_lons)))
        all_lats = [c[0] for c in coords]
        all_lons = [c[1] for c in coords]
    
    return all_lats, all_lons


def find_variable(ds, var_candidates):
    """Find the first matching variable from a list of candidates."""
    for var in var_candidates:
        if var in ds.variables:
            return var
    return None


def fetch_meps25_opendap(date_str, cycle, cache_dir='weather_data/meps25_cache'):
    """
    Fetch MEPS 2.5km data via OPeNDAP with server-side subsetting.
    
    Returns an xarray Dataset with the subsetted data.
    """
    os.makedirs(cache_dir, exist_ok=True)
    
    # Parse date
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"Error: Invalid date format for {date_str}")
        return None
    
    # Check for cached file
    cache_file = os.path.join(cache_dir, f"meps25_subset_{date_str}T{cycle}Z.nc")
    if os.path.exists(cache_file):
        print(f"Loading cached data: {cache_file}")
        try:
            return xr.open_dataset(cache_file)
        except Exception as e:
            print(f"Error opening cached file, re-downloading: {e}")
            os.remove(cache_file)
    
    # Try different URL patterns
    url_patterns = [
        f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_sfc_{date_str}T{cycle}Z.ncml",
        f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_2_5km_{date_str}T{cycle}Z.nc",
        f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_ml_{date_str}T{cycle}Z.ncml",
    ]
    
    ds = None
    successful_url = None
    
    for url in url_patterns:
        try:
            print(f"Trying OPeNDAP URL: {url}")
            ds = xr.open_dataset(url)
            successful_url = url
            print(f"Successfully connected!")
            break
        except Exception as e:
            print(f"  -> Failed: {e}")
            continue
    
    if ds is None:
        print(f"Error: Could not connect to data for {date_str} cycle {cycle}")
        return None
    
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
        
        print(f"Subsetting to Scandinavia domain...")
        
        # Handle curvilinear grids (2D lat/lon arrays)
        if len(lats.shape) == 2:
            lat_mask = (lats >= SCANDINAVIA_BOUNDS['lat_min']) & (lats <= SCANDINAVIA_BOUNDS['lat_max'])
            lon_mask = (lons >= SCANDINAVIA_BOUNDS['lon_min']) & (lons <= SCANDINAVIA_BOUNDS['lon_max'])
            combined_mask = lat_mask & lon_mask
            
            y_indices = np.where(combined_mask.any(axis=1))[0]
            x_indices = np.where(combined_mask.any(axis=0))[0]
            
            if len(y_indices) > 0 and len(x_indices) > 0:
                ds = ds.isel(y=slice(y_indices[0], y_indices[-1]+1),
                            x=slice(x_indices[0], x_indices[-1]+1))
        else:
            try:
                ds = ds.sel({
                    lat_var: slice(SCANDINAVIA_BOUNDS['lat_min'], SCANDINAVIA_BOUNDS['lat_max']),
                    lon_var: slice(SCANDINAVIA_BOUNDS['lon_min'], SCANDINAVIA_BOUNDS['lon_max'])
                })
            except Exception as e:
                print(f"Warning: Could not subset: {e}")
    
    # Select limited time steps to reduce download size
    if 'time' in ds.dims and len(ds.time) > 24:
        print(f"Selecting first 24 time steps (from {len(ds.time)} available)")
        ds = ds.isel(time=slice(0, 24))
    
    # Load the data
    print("Downloading subsetted data...")
    try:
        ds = ds.load()
        
        # Cache for future use
        ds.to_netcdf(cache_file)
        file_size_mb = os.path.getsize(cache_file) / (1024 * 1024)
        print(f"Cached to: {cache_file} ({file_size_mb:.1f} MB)")
        
    except Exception as e:
        print(f"Warning: Download/cache failed: {e}")
    
    return ds


def reduce_to_2d(data_array):
    """Reduce a data array to 2D by selecting first index of extra dimensions."""
    data = data_array
    
    height_dims = ['height', 'height0', 'height1', 'height2', 'height3',
                   'surface', 'level', 'hybrid', 'pressure', 'ensemble_member']
    
    for dim in height_dims:
        if dim in data.dims:
            data = data.isel({dim: 0})
    
    for dim in list(data.dims):
        if dim.lower() not in ['latitude', 'longitude', 'x', 'y', 'lat', 'lon']:
            if data.sizes.get(dim, 0) == 1:
                data = data.squeeze(dim)
    
    return data


def plot_single_variable(ds_time, var_name, config, lats, lons, 
                         time_val, plots_dir, turbine_lats, turbine_lons):
    """Create a single variable plot."""
    
    data = reduce_to_2d(ds_time[var_name])
    
    # Apply conversions
    if config.get('convert_to_celsius', False):
        data = data - 273.15
    if config.get('convert_to_hpa', False):
        data = data / 100
    if config.get('multiply_by'):
        data = data * config['multiply_by']
    
    # Create figure
    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent([SCANDINAVIA_BOUNDS['lon_min'], SCANDINAVIA_BOUNDS['lon_max'],
                   SCANDINAVIA_BOUNDS['lat_min'], SCANDINAVIA_BOUNDS['lat_max']],
                  crs=ccrs.PlateCarree())

    # Map features
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
    ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=0.5)
    ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue', alpha=0.3)
    
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Plot data
    try:
        cf = ax.contourf(lons, lats, data.values,
                         transform=ccrs.PlateCarree(),
                         cmap=config['cmap'],
                         levels=config['levels'],
                         extend='both',
                         norm=config.get('norm'))
        fig.colorbar(cf, ax=ax, orientation='vertical',
                     label=f"{config['title']} ({config['unit']})", pad=0.05, shrink=0.8)
    except Exception:
        # Fallback to pcolormesh
        cf = ax.pcolormesh(lons, lats, data.values,
                          transform=ccrs.PlateCarree(),
                          cmap=config['cmap'],
                          vmin=config['levels'][0],
                          vmax=config['levels'][-1])
        fig.colorbar(cf, ax=ax, orientation='vertical',
                    label=f"{config['title']} ({config['unit']})", pad=0.05, shrink=0.8)

    # Turbine markers
    if turbine_lats and turbine_lons:
        ax.scatter(turbine_lons, turbine_lats, c='red', s=50, marker='^',
                   transform=ccrs.PlateCarree(), label='Wind Turbines',
                   edgecolors='black', linewidths=0.5, zorder=10)
        ax.legend(loc='lower right')

    ax.set_title(f"MEPS 2.5km: {config['title']}\n{time_val.strftime('%Y-%m-%d %H:%M UTC')}", 
                 fontsize=14)

    # Save
    plot_filename = f"meps25_{var_name}_{time_val.strftime('%Y%m%d_%H%M')}.png"
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {plot_path}")


def visualize_meps25_opendap(date_str, cycle, output_dir='visualization/plots/meps25_opendap'):
    """
    Main function: fetch MEPS 2.5km data via OPeNDAP and create visualizations.
    """
    print("=" * 60)
    print(f"MEPS 2.5km OPeNDAP Visualization")
    print(f"Date: {date_str}, Cycle: {cycle}Z")
    print("=" * 60)
    
    # Fetch data
    ds = fetch_meps25_opendap(date_str, cycle)
    if ds is None:
        print("Failed to fetch data")
        return
    
    # Get turbine locations
    turbine_lats, turbine_lons = get_turbine_locations()
    if turbine_lats:
        print(f"Loaded {len(turbine_lats)} turbine locations")
    
    # Create output directory
    plots_dir = os.path.join(output_dir, date_str)
    os.makedirs(plots_dir, exist_ok=True)
    
    # Get coordinate arrays
    lat_var = 'latitude' if 'latitude' in ds else 'lat' if 'lat' in ds else None
    lon_var = 'longitude' if 'longitude' in ds else 'lon' if 'lon' in ds else None
    
    if not lat_var or not lon_var:
        print("Error: Could not find lat/lon coordinates")
        return
    
    lats = ds[lat_var].values
    lons = ds[lon_var].values
    
    # Find available variables
    available_vars = {}
    for output_name, candidates in VARIABLE_CANDIDATES.items():
        found = find_variable(ds, candidates)
        if found:
            available_vars[output_name] = found
            print(f"Found: {output_name} <- {found}")
    
    # Compute wind speed if components available
    if 'x_wind_10m' in available_vars and 'y_wind_10m' in available_vars:
        u_var = available_vars['x_wind_10m']
        v_var = available_vars['y_wind_10m']
        ds['wind_speed_10m'] = np.sqrt(ds[u_var]**2 + ds[v_var]**2)
        available_vars['wind_speed_10m'] = 'wind_speed_10m'
        print("Computed: wind_speed_10m")
    
    # Process each time step
    if 'time' in ds.dims:
        time_steps = ds.time.values
    else:
        time_steps = [None]
    
    print(f"\nProcessing {len(time_steps)} time steps...")
    
    for i, time_step in enumerate(time_steps):
        if time_step is not None:
            ds_time = ds.sel(time=time_step)
            time_val = pd.to_datetime(time_step)
        else:
            ds_time = ds
            time_val = datetime.strptime(f"{date_str}{cycle}", '%Y%m%d%H')
        
        print(f"\n[{i+1}/{len(time_steps)}] {time_val.strftime('%Y-%m-%d %H:%M UTC')}")
        
        for output_name, actual_var in available_vars.items():
            if output_name in PLOT_CONFIG:
                config = PLOT_CONFIG[output_name]
                try:
                    plot_single_variable(ds_time, actual_var if actual_var != output_name else output_name,
                                        config, lats, lons, time_val, plots_dir,
                                        turbine_lats, turbine_lons)
                except Exception as e:
                    print(f"  Error plotting {output_name}: {e}")
    
    ds.close()
    print("\n" + "=" * 60)
    print(f"Done! Plots saved to: {plots_dir}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Fetch MEPS 2.5km data via OPeNDAP and create visualizations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Visualize a specific date
  python visualize_meps25_opendap.py --date 20231115 --cycle 06
  
  # Use custom output directory
  python visualize_meps25_opendap.py --date 20231115 --cycle 06 --output-dir my_plots
"""
    )
    parser.add_argument('--date', type=str, required=True,
                        help='Date to process (YYYYMMDD format)')
    parser.add_argument('--cycle', type=str, default='06',
                        help='Forecast cycle (default: 06)')
    parser.add_argument('--output-dir', type=str, default='visualization/plots/meps25_opendap',
                        help='Output directory for plots')
    args = parser.parse_args()
    
    visualize_meps25_opendap(args.date, args.cycle, args.output_dir)


if __name__ == "__main__":
    main()
