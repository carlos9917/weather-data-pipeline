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

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import EUROPE_BOUNDS

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

def plot_map(ds_single, var_key, config, plots_dir, time_val):
    """
    Generates and saves a single map plot for a given variable and time step.
    """
    var_name_to_plot = config.get('speed_var', var_key)
    
    # Select data for the specific time and squeeze out other dims like init_time
    data_array = ds_single[var_name_to_plot]
    
    # Ensure data is 2D [lat, lon] by selecting first element from other dimensions
    squeezable_dims = [dim for dim in data_array.dims if dim not in ['latitude', 'longitude']]
    if squeezable_dims:
        selection = {dim: 0 for dim in squeezable_dims}
        data = data_array.isel(**selection).squeeze()
    else:
        data = data_array.squeeze()

    # Unit conversions
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

    # Contour plot
    cf = ax.contourf(ds_single['longitude'], ds_single['latitude'], data,
                     transform=ccrs.PlateCarree(),
                     cmap=config['cmap'],
                     levels=config['levels'],
                     extend='both',
                     norm=config.get('norm'))
    fig.colorbar(cf, ax=ax, orientation='vertical',
                 label=f"{config['title']} ({config['unit']})", pad=0.05)

    # Wind barbs
    if 'u_var' in config and 'v_var' in config:
        u_wind_array = ds_single[config['u_var']]
        v_wind_array = ds_single[config['v_var']]
        
        if squeezable_dims:
            u_wind = u_wind_array.isel(**selection).squeeze()
            v_wind = v_wind_array.isel(**selection).squeeze()
        else:
            u_wind = u_wind_array.squeeze()
            v_wind = v_wind_array.squeeze()
            
        skip = max(1, len(ds_single['longitude']) // 25)
        ax.barbs(ds_single['longitude'][::skip], ds_single['latitude'][::skip],
                 u_wind[::skip, ::skip], v_wind[::skip, ::skip],
                 length=6, transform=ccrs.PlateCarree(), pivot='middle')

    ax.set_title(f"{config['title']}\n{time_val.strftime('%Y-%m-%d %H:%M UTC')}", fontsize=16)

    plot_filename = f"{var_name_to_plot}_{time_val.strftime('%Y%m%d_%H%M')}.png"
    plot_path = os.path.join(plots_dir, plot_filename)

    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved plot: {plot_path}")

def create_visualizations(date_str, cycle):
    """
    Creates map visualizations for all configured variables from the Zarr store.
    """
    zarr_store_path = os.path.join('data', 'processed', f'gfs_{date_str}_{cycle}.zarr')
    if not os.path.exists(zarr_store_path):
        print(f"Error: Zarr store not found at {zarr_store_path}")
        return

    plots_dir = os.path.join('visualization', 'plots', date_str, cycle)
    os.makedirs(plots_dir, exist_ok=True)
    
    try:
        ds = xr.open_zarr(zarr_store_path)
        ds_cycle = ds.isel(init_time=0) if 'init_time' in ds.dims else ds

        if not ds_cycle.time.size:
            print(f"No data found for date {date_str} and cycle {cycle}.")
            return

        print(f"Available variables in Zarr store: {list(ds_cycle.variables)}")
        print(f"Processing {ds_cycle.time.size} time steps...")

        for time_step in ds_cycle.time:
            ds_single = ds_cycle.sel(time=time_step)
            time_val = pd.to_datetime(time_step.values)
            
            for var_key, config in PLOT_CONFIG.items():
                required_vars = [config.get('speed_var', var_key)]
                if 'u_var' in config: required_vars.append(config['u_var'])
                if 'v_var' in config: required_vars.append(config['v_var'])
                
                if all(v in ds_single for v in required_vars):
                    try:
                        plot_map(ds_single, var_key, config, plots_dir, time_val)
                    except Exception as e:
                        print(f"Failed to create plot for {var_key} at {time_val.strftime('%Y-%m-%d %H:%M')}: {e}")
                else:
                    print(f"Skipping plot for '{config['title']}' at {time_val.strftime('%Y-%m-%d %H:%M')}: missing required data variables.")

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
