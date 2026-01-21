"""
MEPS 2.5km Visualizations

Creates map visualizations from processed MEPS 2.5km Zarr data.
Uses the same style as create_met_visualizations.py with individual plots per variable.
Includes turbine location markers.
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

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SCANDINAVIA_BOUNDS, TURBINE_DATA_PATH

# Define plotting configurations for each variable
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
        'levels': np.linspace(0, 1, 11),
        'multiply_by': 100  # Convert fraction to percent for display
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
        except Exception as e:
            print(f"Warning: Could not read {csv_file}: {e}")
    
    # Remove duplicates
    if all_lats and all_lons:
        coords = list(set(zip(all_lats, all_lons)))
        all_lats = [c[0] for c in coords]
        all_lons = [c[1] for c in coords]
    
    return all_lats, all_lons


def reduce_to_2d(data_array):
    """Reduce a data array to 2D by selecting first index of extra dimensions."""
    data = data_array
    
    # Common height/level dimensions to reduce
    height_dims = ['height', 'height0', 'height1', 'height2', 'height3',
                   'surface', 'level', 'hybrid', 'pressure', 'ensemble_member']
    
    for dim in height_dims:
        if dim in data.dims:
            data = data.isel({dim: 0})
    
    # Squeeze any remaining singleton dimensions (except lat/lon/x/y)
    for dim in list(data.dims):
        if dim.lower() not in ['latitude', 'longitude', 'x', 'y', 'lat', 'lon']:
            if data.sizes[dim] == 1:
                data = data.squeeze(dim)
    
    return data


def plot_map(ds_single, var_name, config, plots_dir, time_val, turbine_lats, turbine_lons):
    """
    Generates and saves a single map plot for a given variable and time step.
    """
    data_array = ds_single[var_name]
    
    # Reduce to 2D
    data = reduce_to_2d(data_array)
    
    # Apply unit conversions
    if config.get('convert_to_celsius', False):
        data = data - 273.15
    if config.get('convert_to_hpa', False):
        data = data / 100
    if config.get('multiply_by'):
        data = data * config['multiply_by']
    
    # Get coordinate arrays
    if 'latitude' in ds_single.coords:
        lats = ds_single['latitude'].values
        lons = ds_single['longitude'].values
    elif 'lat' in ds_single.coords:
        lats = ds_single['lat'].values
        lons = ds_single['lon'].values
    else:
        # Try x, y with latitude/longitude as 2D variables
        lats = ds_single['latitude'].values if 'latitude' in ds_single else None
        lons = ds_single['longitude'].values if 'longitude' in ds_single else None
    
    if lats is None or lons is None:
        print(f"Could not find lat/lon coordinates for {var_name}")
        return
    
    # Create figure
    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent([SCANDINAVIA_BOUNDS['lon_min'], SCANDINAVIA_BOUNDS['lon_max'],
                   SCANDINAVIA_BOUNDS['lat_min'], SCANDINAVIA_BOUNDS['lat_max']],
                  crs=ccrs.PlateCarree())

    # Add map features
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
    ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=0.5)
    ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue', alpha=0.3)
    
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Create contour plot
    try:
        cf = ax.contourf(lons, lats, data.values,
                         transform=ccrs.PlateCarree(),
                         cmap=config['cmap'],
                         levels=config['levels'],
                         extend='both',
                         norm=config.get('norm'))
        
        fig.colorbar(cf, ax=ax, orientation='vertical',
                     label=f"{config['title']} ({config['unit']})", pad=0.05, shrink=0.8)
    except Exception as e:
        print(f"Warning: Could not create contourf for {var_name}: {e}")
        # Try pcolormesh as fallback
        try:
            cf = ax.pcolormesh(lons, lats, data.values,
                              transform=ccrs.PlateCarree(),
                              cmap=config['cmap'],
                              vmin=config['levels'][0],
                              vmax=config['levels'][-1])
            fig.colorbar(cf, ax=ax, orientation='vertical',
                        label=f"{config['title']} ({config['unit']})", pad=0.05, shrink=0.8)
        except Exception as e2:
            print(f"Error: Could not plot {var_name}: {e2}")
            plt.close(fig)
            return

    # Add turbine locations
    if turbine_lats and turbine_lons:
        ax.scatter(turbine_lons, turbine_lats, c='red', s=50, marker='^',
                   transform=ccrs.PlateCarree(), label='Wind Turbines',
                   edgecolors='black', linewidths=0.5, zorder=10)
        ax.legend(loc='lower right')

    # Set title
    ax.set_title(f"MEPS 2.5km: {config['title']}\n{time_val.strftime('%Y-%m-%d %H:%M UTC')}", 
                 fontsize=14)

    # Save plot
    plot_filename = f"meps25_{var_name}_{time_val.strftime('%Y%m%d_%H%M')}.png"
    plot_path = os.path.join(plots_dir, plot_filename)

    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved plot: {plot_path}")


def create_meps25_visualizations(date_str, cycle):
    """
    Creates map visualizations for all configured variables from the MEPS 2.5km Zarr store.
    """
    zarr_store_path = os.path.join('data', 'processed', f'meps25_data_{date_str}_{cycle}.zarr')
    if not os.path.exists(zarr_store_path):
        print(f"Error: MEPS 2.5km Zarr store not found at {zarr_store_path}")
        return

    plots_dir = os.path.join('visualization', 'plots', 'meps25', date_str, cycle)
    os.makedirs(plots_dir, exist_ok=True)
    
    # Get turbine locations
    turbine_lats, turbine_lons = get_turbine_locations()
    if turbine_lats:
        print(f"Loaded {len(turbine_lats)} turbine locations")
    else:
        print("No turbine locations found (plots will be created without markers)")
    
    try:
        ds = xr.open_zarr(zarr_store_path)
        ds_cycle = ds.isel(init_time=0) if 'init_time' in ds.dims else ds

        if 'time' not in ds_cycle.dims or not ds_cycle.time.size:
            print(f"No time data found for date {date_str} and cycle {cycle}.")
            return

        print(f"Available variables: {list(ds_cycle.data_vars)}")
        print(f"Processing {ds_cycle.time.size} time steps...")

        for time_step in ds_cycle.time:
            ds_single = ds_cycle.sel(time=time_step)
            time_val = pd.to_datetime(time_step.values)
            time_str = time_val.strftime('%Y-%m-%d %H:%M UTC')
            
            for var_name, config in PLOT_CONFIG.items():
                if var_name in ds_single:
                    try:
                        plot_map(ds_single, var_name, config, plots_dir, time_val,
                                turbine_lats, turbine_lons)
                    except Exception as e:
                        print(f"Failed to create plot for {var_name} at {time_str}: {e}")
                else:
                    print(f"Skipping '{config['title']}' at {time_str}: variable not in dataset")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create map visualizations from MEPS 2.5km Zarr data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python create_meps25_visualizations.py --date 20231115 --cycle 06
"""
    )
    parser.add_argument("--date", required=True, help="Start date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    create_meps25_visualizations(args.date, args.cycle)
