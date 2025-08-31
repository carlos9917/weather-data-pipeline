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
            ax.set_extent([EUROPE_BOUNDS['lon_min'], EUROPE_BOUNDS['lon_max'], 
                          EUROPE_BOUNDS['lat_min'], EUROPE_BOUNDS['lat_max']], 
                         crs=ccrs.PlateCarree())

            ax.add_feature(cfeature.COASTLINE)
            ax.add_feature(cfeature.BORDERS, linestyle=':')
            ax.gridlines(draw_labels=True)

            # Debug: Print data info
            print(f"Data points: {len(df)}")
            print(f"Unique lons: {len(df['longitude'].unique())}")
            print(f"Unique lats: {len(df['latitude'].unique())}")
            
            # Create proper grid using pivot_table
            try:
                # Sort data to ensure proper ordering
                df_sorted = df.sort_values(['latitude', 'longitude'])
                
                # Create grids using pivot_table
                wind_speed_grid = df.pivot_table(
                    values='wind_speed', 
                    index='latitude', 
                    columns='longitude', 
                    aggfunc='mean'  # Handle duplicates by averaging
                )
                
                u_wind_grid = df.pivot_table(
                    values='u_wind', 
                    index='latitude', 
                    columns='longitude', 
                    aggfunc='mean'
                )
                
                v_wind_grid = df.pivot_table(
                    values='v_wind', 
                    index='latitude', 
                    columns='longitude', 
                    aggfunc='mean'
                )
                
                # Get coordinate arrays
                lons = wind_speed_grid.columns.values
                lats = wind_speed_grid.index.values
                
                # Convert to numpy arrays
                wind_speed_data = wind_speed_grid.values
                u_wind_data = u_wind_grid.values
                v_wind_data = v_wind_grid.values
                
                # Plot wind speed contours
                cf = ax.contourf(lons, lats, wind_speed_data, 
                               transform=ccrs.PlateCarree(), 
                               cmap='viridis', 
                               levels=np.linspace(0, 25, 11))
                fig.colorbar(cf, ax=ax, orientation='vertical', label='Wind Speed (m/s)')

                # Plot wind barbs (with proper subsampling)
                skip = max(1, len(lons)//20)  # Adaptive skip based on data density
                lon_sub = lons[::skip]
                lat_sub = lats[::skip]
                u_sub = u_wind_data[::skip, ::skip]
                v_sub = v_wind_data[::skip, ::skip]
                
                ax.barbs(lon_sub, lat_sub, u_sub, v_sub, 
                        length=6, transform=ccrs.PlateCarree())

                ax.set_title(f'Wind Speed and Direction for {time}')
                
                plot_path = os.path.join(plots_dir, f'wind_map_{time.strftime("%Y%m%d%H%M")}.png')
                plt.savefig(plot_path, dpi=150, bbox_inches='tight')
                plt.close(fig)
                print(f"Saved plot: {plot_path}")
                
            except Exception as grid_error:
                print(f"Grid creation failed: {grid_error}")
                # Fallback: scatter plot
                fig, ax = plt.subplots(figsize=(12, 10), 
                                     subplot_kw={'projection': ccrs.PlateCarree()})
                ax.set_extent([EUROPE_BOUNDS['lon_min'], EUROPE_BOUNDS['lon_max'], 
                              EUROPE_BOUNDS['lat_min'], EUROPE_BOUNDS['lat_max']], 
                             crs=ccrs.PlateCarree())
                
                ax.add_feature(cfeature.COASTLINE)
                ax.add_feature(cfeature.BORDERS, linestyle=':')
                ax.gridlines(draw_labels=True)
                
                # Scatter plot for wind speed
                scatter = ax.scatter(df['longitude'], df['latitude'], 
                                   c=df['wind_speed'], cmap='viridis',
                                   transform=ccrs.PlateCarree(), s=1)
                fig.colorbar(scatter, ax=ax, orientation='vertical', label='Wind Speed (m/s)')
                
                ax.set_title(f'Wind Speed (Scatter) for {time}')
                
                plot_path = os.path.join(plots_dir, f'wind_scatter_{time.strftime("%Y%m%d%H%M")}.png')
                plt.savefig(plot_path, dpi=150, bbox_inches='tight')
                plt.close(fig)
                print(f"Saved fallback plot: {plot_path}")

    except Exception as e:
        print(f"Error creating visualizations: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create wind map visualizations.")
    parser.add_argument("--date", required=True, help="Date in YYYYMMDD format.")
    parser.add_argument("--cycle", required=True, help="Cycle (00, 06, 12, 18).")
    args = parser.parse_args()

    create_visualizations(args.date, args.cycle)
