import os
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import argparse
import sys
import numpy as np
import pandas as pd

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import ZARR_STORE_PATH, EUROPE_BOUNDS

# Define plotting configurations for each variable
PLOT_CONFIG = {
    'temperature': {
        'title': 'Temperature at 2m',
        'unit': '°C',
        'cmap': 'coolwarm',
        'levels': np.linspace(-10, 35, 21),
        'convert_to_celsius': True
    },
    'precipitation_rate': {
        'title': 'Precipitation Rate',
        'unit': 'mm/hr',
        'cmap': 'Blues',
        'levels': np.linspace(0, 10, 11),
        'norm': mcolors.BoundaryNorm(boundaries=[0, 0.1, 0.5, 1, 2, 5, 10], ncolors=256)
    },
    'cloud_cover': {
        'title': 'Total Cloud Cover',
        'unit': '%',
        'cmap': 'Greys_r',
        'levels': np.linspace(0, 100, 11)
    },
    'surface_pressure': {
        'title': 'Surface Pressure',
        'unit': 'hPa',
        'cmap': 'viridis',
        'levels': np.linspace(980, 1050, 15),
        'convert_to_hpa': True
    },
    'wind_10m': {
        'title': 'Wind at 10m',
        'unit': 'm/s',
        'cmap': 'viridis',
        'levels': np.linspace(0, 25, 11),
        'speed_var': 'wind_speed_10m',
        'u_var': 'u_wind_10m',
        'v_var': 'v_wind_10m'
    },
    'wind_100m': {
        'title': 'Wind at 100m',
        'unit': 'm/s',
        'cmap': 'plasma',
        'levels': np.linspace(0, 35, 15),
        'speed_var': 'wind_speed_100m',
        'u_var': 'u_wind_100m',
        'v_var': 'v_wind_100m'
    }
}

def plot_map(ds_time, config, plots_dir, time_str):
    """
    Creates and saves a single map visualization for a given variable and time.
    """
    var_name = config.get('speed_var', list(PLOT_CONFIG.keys())[list(PLOT_CONFIG.values()).index(config)])
    data = ds_time[var_name].squeeze()

    if config.get('convert_to_celsius', False):
        data = data - 273.15
    if config.get('convert_to_hpa', False):
        data = data / 100

    fig = plt.figure(figsize=(14, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.set_extent([EUROPE_BOUNDS['lon_min'], EUROPE_BOUNDS['lon_max'],
                   EUROPE_BOUNDS['lat_min'], EUROPE_BOUNDS['lat_max']],
                  crs=ccrs.PlateCarree())

    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Plot contour fill
    cf = ax.contourf(ds_time['longitude'], ds_time['latitude'], data,
                    transform=ccrs.PlateCarree(),
                    cmap=config['cmap'],
                    levels=config['levels'],
                    extend='both',
                    norm=config.get('norm'))
    
    fig.colorbar(cf, ax=ax, orientation='vertical', label=f"{config['title']} ({config['unit']})", pad=0.05)

    # Plot wind barbs for wind variables
    if 'u_var' in config and 'v_var' in config:
        u_wind = ds_time[config['u_var']].squeeze()
        v_wind = ds_time[config['v_var']].squeeze()
        
        # Subsample for clarity
        skip = max(1, len(ds_time['longitude']) // 25)
        ax.barbs(ds_time['longitude'][::skip], ds_time['latitude'][::skip],
                 u_wind[::skip, ::skip], v_wind[::skip, ::skip],
                 length=6, transform=ccrs.PlateCarree(), pivot='middle')

    ax.set_title(f"{config['title']}\n{time_str}", fontsize=16)
    
    plot_filename = f"{var_name}_{pd.to_datetime(str(ds_time.time.values)).strftime('%Y%m%d_%H%M')}.png"
    plot_path = os.path.join(plots_dir, plot_filename)
    
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved plot: {plot_path}")

def create_visualizations(date_str, cycle):
    """
    Creates map visualizations for all configured variables from the Zarr store.
    """
    if not os.path.exists(ZARR_STORE_PATH):
        print(f"Error: Zarr store not found at {ZARR_STORE_PATH}")
        return

    plots_dir = os.path.join('visualization', 'plots', date_str, cycle)
    os.makedirs(plots_dir, exist_ok=True)
    
    try:
        ds = xr.open_zarr(ZARR_STORE_PATH)
        
        # Select data for the specified date and cycle
        start_time = pd.to_datetime(f"{date_str} {cycle}:00")
        end_time = start_time + pd.Timedelta(hours=72)
        ds_cycle = ds.sel(time=slice(start_time, end_time))

        if not ds_cycle.time.size:
            print(f"No data found for date {date_str} and cycle {cycle}.")
            return

        print(f"Processing {ds_cycle.time.size} time steps...")

        for time_step in ds_cycle.time:
            ds_time = ds_cycle.sel(time=time_step)
            time_str = pd.to_datetime(str(time_step.values)).strftime('%Y-%m-%d %H:%M UTC')
            
            for var_key, config in PLOT_CONFIG.items():
                # Check if all required variables for the plot exist in the dataset
                required_vars = [config.get('speed_var', var_key)]
                if 'u_var' in config: required_vars.append(config['u_var'])
                if 'v_var' in config: required_vars.append(config['v_var'])
                
                if all(v in ds_time for v in required_vars):
                    try:
                        plot_map(ds_time, config, plots_dir, time_str)
                    except Exception as e:
                        print(f"Failed to create plot for {var_key} at {time_str}: {e}")
                else:
                    print(f"Skipping plot for '{config['title']}' at {time_str}: missing required data variables.")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create map visualizations from GFS Zarr data.")
    parser.add_argument("--date", required=True, help="Start date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    create_visualizations(args.date, args.cycle)