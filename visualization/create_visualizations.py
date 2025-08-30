
import os
import duckdb
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import argparse
import sys
import numpy as np

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATABASE_PATH, EUROPE_BOUNDS

def create_visualizations(date_str, cycle):
    """
    Creates map visualizations of wind speed and direction.

    Args:
        date_str (str): The date in YYYYMMDD format.
        cycle (str): The cycle ('00', '06', '12', '18').
    """
    plots_dir = os.path.join('visualization', 'plots', date_str, cycle)
    os.makedirs(plots_dir, exist_ok=True)

    conn = duckdb.connect(DATABASE_PATH)
    
    try:
        times = conn.execute(f"""
            SELECT DISTINCT time 
            FROM gfs_data 
            WHERE time::date = '{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}' 
              AND EXTRACT(hour FROM time) >= {int(cycle)}
            ORDER BY time
        """).fetchdf()['time'].tolist()

        for time in times:
            df = conn.execute(f"SELECT * FROM gfs_data WHERE time = '{time}'").fetchdf()

            if df.empty:
                continue

            fig = plt.figure(figsize=(12, 10))
            ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
            ax.set_extent([EUROPE_BOUNDS['lon_min'], EUROPE_BOUNDS['lon_max'], EUROPE_BOUNDS['lat_min'], EUROPE_BOUNDS['lat_max']], crs=ccrs.PlateCarree())

            ax.add_feature(cfeature.COASTLINE)
            ax.add_feature(cfeature.BORDERS, linestyle=':')
            ax.gridlines(draw_labels=True)

            # Plot wind speed contours
            lons = df['longitude'].unique()
            lats = df['latitude'].unique()
            wind_speed = df['wind_speed'].values.reshape(len(lats), len(lons))
            
            cf = ax.contourf(lons, lats, wind_speed, transform=ccrs.PlateCarree(), cmap='viridis', levels=np.linspace(0, 25, 11))
            fig.colorbar(cf, ax=ax, orientation='vertical', label='Wind Speed (m/s)')

            # Plot wind barbs
            skip = 10  # Skip points to avoid overcrowding
            ax.barbs(lons[::skip], lats[::skip], df['u_wind'].values.reshape(len(lats), len(lons))[::skip, ::skip], df['v_wind'].values.reshape(len(lats), len(lons))[::skip, ::skip], length=6, transform=ccrs.PlateCarree())

            ax.set_title(f'Wind Speed and Direction for {time}')
            
            plot_path = os.path.join(plots_dir, f'wind_map_{time.strftime("%Y%m%d%H%M")}.png')
            plt.savefig(plot_path)
            plt.close(fig)
            print(f"Saved plot: {plot_path}")

    except Exception as e:
        print(f"Error creating visualizations: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create wind map visualizations.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    create_visualizations(args.date, args.cycle)
