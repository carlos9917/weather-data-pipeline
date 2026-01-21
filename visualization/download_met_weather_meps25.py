import pandas as pd
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import os
import glob
from pathlib import Path
from datetime import datetime, timedelta
import argparse

# MEPS 2.5km archive on MET Norway THREDDS
# Use OPeNDAP (dodsC) for server-side subsetting - much smaller downloads!
MEPS25_OPENDAP_URL = "https://thredds.met.no/thredds/dodsC/meps25epsarchive"
# File naming pattern for deterministic surface data
MEPS25_FILE_TEMPLATE = "meps_det_2_5km_{date}T{cycle}Z.nc"

# Variable name candidates for each parameter
TEMP_VARS = ['air_temperature_2m', 'air_temperature_ml', 't2m', 'temperature_2m']
U_WIND_VARS = ['x_wind_10m', 'x_wind_ml', 'u10', 'eastward_wind_10m']
V_WIND_VARS = ['y_wind_10m', 'y_wind_ml', 'v10', 'northward_wind_10m']
PRES_VARS = ['surface_air_pressure', 'air_pressure_at_sea_level', 'msl', 'ps']
PRECIP_VARS = ['precipitation_amount_acc', 'precipitation_amount', 
               'lwe_precipitation_rate', 'precipitation_rate',
               'integral_of_surface_downward_precipitation_flux_wrt_time']
PWV_VARS = ['atmosphere_water_vapor_content', 
            'integral_of_atmosphere_water_vapor_content_wrt_time',
            'precipitable_water', 'total_precipitable_water',
            'atmosphere_mass_content_of_water_vapor',
            'lwe_thickness_of_atmosphere_mass_content_of_water_vapor']


def get_turbine_domain(buffer_deg=1.5):
    """Read turbine CSV files and calculate bounding box with buffer."""
    data_path = "processed_data"
    csv_files = glob.glob(os.path.join(data_path, '*_processed.csv.gz'))
    
    all_lats = []
    all_lons = []
    
    for csv_file in csv_files:
        df = pd.read_csv(csv_file, compression='gzip')
        unique_turbines = df[['lat', 'lon']].drop_duplicates()
        all_lats.extend(unique_turbines['lat'].tolist())
        all_lons.extend(unique_turbines['lon'].tolist())
    
    domain = {
        'lat_min': min(all_lats) - buffer_deg,
        'lat_max': max(all_lats) + buffer_deg,
        'lon_min': min(all_lons) - buffer_deg,
        'lon_max': max(all_lons) + buffer_deg
    }
    
    print(f"Turbine domain: lat [{domain['lat_min']:.2f}, {domain['lat_max']:.2f}], "
          f"lon [{domain['lon_min']:.2f}, {domain['lon_max']:.2f}]")
    
    return domain, all_lats, all_lons


def get_interesting_dates(analysis_dir='analysis_plots'):
    """Parse analysis plots to find dates with power drop events."""
    # Auto-fallback: many workflows write drop plots under processed_data/analysis_plots
    candidate_dirs = [analysis_dir, os.path.join('processed_data', analysis_dir)]
    analysis_dir_used = None
    plot_files = []
    
    for d in candidate_dirs:
        files = glob.glob(os.path.join(d, '*_drop_*.png'))
        if files:
            analysis_dir_used = d
            plot_files = files
            break
    
    if analysis_dir_used is None:
        analysis_dir_used = analysis_dir
    
    date_info = []
    for plot_file in plot_files:
        filename = os.path.basename(plot_file)
        parts = filename.split('_')
        if len(parts) >= 4:
            park = parts[0]
            date_str = parts[2]
            time_str = parts[3].replace('.png', '')
            if len(date_str) == 8 and date_str.isdigit():
                date_info.append({
                    'date': date_str,
                    'time': time_str,
                    'park': park,
                    'filename': filename
                })
    
    df = pd.DataFrame(date_info)

    if df.empty or 'date' not in df.columns:
        print(f"Found 0 power drop events (looked in: {candidate_dirs})")
        return df, pd.Series(dtype='int64')

    date_counts = df['date'].value_counts()

    print(f"Found {len(df)} power drop events across {len(date_counts)} unique dates (from: {analysis_dir_used})")
    print(f"\nTop dates by number of events:")
    print(date_counts.head(20))
    
    return df, date_counts


def select_most_interesting_dates(date_counts, n_dates=10):
    """Select dates with the most power drop events."""
    top_dates = date_counts.head(n_dates).index.tolist()
    print(f"\nSelected {n_dates} most interesting dates (most events):")
    for i, date in enumerate(top_dates, 1):
        print(f"  {i}. {date} ({date_counts[date]} events)")
    
    return top_dates


def fetch_and_subset_meps25(date_str, cycle='06', domain=None, output_dir='weather_data/meps25'):
    """
    Fetch MEPS 2.5km data via OPeNDAP with server-side subsetting.
    
    This uses OPeNDAP to subset the data on the server before downloading,
    dramatically reducing the download size (from ~70GB to a few MB).
    """
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"Error: Invalid date format for {date_str}")
        return None
    
    # Output file for subsetted data
    output_file = os.path.join(output_dir, f"meps25_subset_{date_str}T{cycle}Z.nc")
    
    if os.path.exists(output_file):
        print(f"Subsetted file already exists: {output_file}")
        try:
            return xr.open_dataset(output_file)
        except Exception as e:
            print(f"Error opening cached file, re-downloading: {e}")
            os.remove(output_file)
    
    # Try different file naming patterns via OPeNDAP
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
            print(f"Successfully connected to: {url}")
            break
        except Exception as e:
            print(f"Failed: {e}")
            continue
    
    if ds is None:
        print(f"Error: Could not connect to data for {date_str} from any URL pattern")
        return None
    
    print(f"\nAvailable variables: {list(ds.variables.keys())[:15]}...")
    if len(ds.variables) > 15:
        print(f"  ... and {len(ds.variables) - 15} more")
    
    # Subset to domain if provided
    if domain:
        print(f"\nSubsetting to domain: lat [{domain['lat_min']:.2f}, {domain['lat_max']:.2f}], "
              f"lon [{domain['lon_min']:.2f}, {domain['lon_max']:.2f}]")
        
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
        
        if lat_var and lon_var:
            lats = ds[lat_var].values
            lons = ds[lon_var].values
            
            # Handle curvilinear grids (2D lat/lon arrays)
            if len(lats.shape) == 2:
                # Create mask for domain
                lat_mask = (lats >= domain['lat_min']) & (lats <= domain['lat_max'])
                lon_mask = (lons >= domain['lon_min']) & (lons <= domain['lon_max'])
                combined_mask = lat_mask & lon_mask
                
                # Find y and x indices that contain valid data
                y_indices = np.where(combined_mask.any(axis=1))[0]
                x_indices = np.where(combined_mask.any(axis=0))[0]
                
                if len(y_indices) > 0 and len(x_indices) > 0:
                    print(f"Selecting y indices {y_indices[0]}:{y_indices[-1]+1}, "
                          f"x indices {x_indices[0]}:{x_indices[-1]+1}")
                    ds = ds.isel(y=slice(y_indices[0], y_indices[-1]+1),
                                x=slice(x_indices[0], x_indices[-1]+1))
                else:
                    print(f"Warning: No data in specified domain, using full domain")
            else:
                # 1D coordinates - use sel with slice
                try:
                    ds = ds.sel({
                        lat_var: slice(domain['lat_min'], domain['lat_max']),
                        lon_var: slice(domain['lon_min'], domain['lon_max'])
                    })
                except Exception as e:
                    print(f"Warning: Could not subset using sel: {e}")
        else:
            print(f"Warning: Could not find lat/lon coordinates")
    
    # Select first time step if multiple
    if 'time' in ds.dims and len(ds.time) > 0:
        print(f"Selecting first time step from {len(ds.time)} available")
        ds = ds.isel(time=0)
    
    # Load the subsetted data into memory and save locally
    print(f"\nDownloading subsetted data...")
    try:
        ds = ds.load()  # This triggers the actual download
        
        # Save to local file for caching
        ds.to_netcdf(output_file)
        print(f"Saved subsetted data to: {output_file}")
        
        # Report file size
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"File size: {file_size_mb:.1f} MB")
        
    except Exception as e:
        print(f"Warning: Could not save to file: {e}")
    
    return ds


def find_variable(ds, var_candidates):
    """Find the first matching variable from a list of candidates."""
    for var in var_candidates:
        if var in ds.variables:
            return var
    return None


def list_available_variables(date_str, cycle='06'):
    """Connect to a dataset and list all available variables."""
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"Error: Invalid date format for {date_str}")
        return
    
    url = f"{MEPS25_OPENDAP_URL}/{year}/{month}/{day}/meps_det_sfc_{date_str}T{cycle}Z.ncml"
    
    print(f"Connecting to: {url}")
    try:
        ds = xr.open_dataset(url)
        print(f"\nAll available variables ({len(ds.variables)}):")
        print("-" * 50)
        for var in sorted(ds.variables.keys()):
            if var in ds.coords:
                print(f"  [coord] {var}: {ds[var].dims}")
            else:
                print(f"  {var}: {ds[var].dims}")
        ds.close()
    except Exception as e:
        print(f"Error: {e}")


def reduce_to_2d(data_array, ds):
    """Reduce a data array to 2D by selecting first index of extra dimensions."""
    data = data_array
    
    # Common height/level dimensions to reduce
    height_dims = ['height', 'height0', 'height1', 'height2', 'height3',
                   'surface', 'level', 'hybrid', 'pressure']
    
    for dim in height_dims:
        if dim in data.dims:
            data = data.isel({dim: 0})
    
    values = data.values
    
    # Further reduce if still > 2D
    while values.ndim > 2:
        values = values[0]
    
    return values


def plot_weather_meps25(ds, date_str, turbine_lats, turbine_lons, output_dir='weather_plots/meps25'):
    """Create weather plots including temperature, wind, pressure, precipitation, and PWV."""
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Find coordinate variables
        lat_var = None
        lon_var = None
        for var in ['latitude', 'lat', 'y']:
            if var in ds.coords:
                lat_var = var
                break
        for var in ['longitude', 'lon', 'x']:
            if var in ds.coords:
                lon_var = var
                break
        
        if not lat_var or not lon_var:
            print(f"Error: Could not find lat/lon coordinates in dataset")
            return
        
        lats = ds[lat_var].values
        lons = ds[lon_var].values
        
        # Find variables
        temp_var = find_variable(ds, TEMP_VARS)
        u_var = find_variable(ds, U_WIND_VARS)
        v_var = find_variable(ds, V_WIND_VARS)
        pres_var = find_variable(ds, PRES_VARS)
        precip_var = find_variable(ds, PRECIP_VARS)
        pwv_var = find_variable(ds, PWV_VARS)
        
        print(f"Using variables: temp={temp_var}, u={u_var}, v={v_var}, "
              f"pres={pres_var}, precip={precip_var}, pwv={pwv_var}")
        
        # Create 3x2 subplot figure
        fig = plt.figure(figsize=(16, 18))
        
        # 1. Temperature plot
        if temp_var:
            ax1 = plt.subplot(3, 2, 1, projection=ccrs.PlateCarree())
            temp = reduce_to_2d(ds[temp_var], ds)
            
            # Convert from Kelvin if needed
            if temp.max() > 200:
                temp = temp - 273.15
            
            ax1.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
            ax1.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax1.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=':')
            ax1.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
            
            cf = ax1.contourf(lons, lats, temp, levels=15, cmap='RdYlBu_r', transform=ccrs.PlateCarree())
            plt.colorbar(cf, ax=ax1, label='Temperature (°C)', shrink=0.8)
            ax1.scatter(turbine_lons, turbine_lats, c='black', s=20, marker='o', transform=ccrs.PlateCarree())
            ax1.set_title(f'2m Temperature - {date_str}')
        
        # 2. Wind speed and direction plot
        if u_var and v_var:
            ax2 = plt.subplot(3, 2, 2, projection=ccrs.PlateCarree())
            u = reduce_to_2d(ds[u_var], ds)
            v = reduce_to_2d(ds[v_var], ds)
            wind_speed = np.sqrt(u**2 + v**2)
            
            ax2.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
            ax2.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax2.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=':')
            ax2.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
            
            cf = ax2.contourf(lons, lats, wind_speed, levels=15, cmap='viridis', transform=ccrs.PlateCarree())
            plt.colorbar(cf, ax=ax2, label='Wind Speed (m/s)', shrink=0.8)
            
            # Add wind vectors
            skip = max(1, min(u.shape[0], u.shape[1]) // 20)
            u_quiver = u[::skip, ::skip]
            v_quiver = v[::skip, ::skip]
            
            if len(lons.shape) == 2:
                lons_quiver = lons[::skip, ::skip]
                lats_quiver = lats[::skip, ::skip]
            else:
                lons_1d = lons[::skip]
                lats_1d = lats[::skip]
                lons_quiver, lats_quiver = np.meshgrid(lons_1d, lats_1d)
            
            ax2.quiver(lons_quiver, lats_quiver, u_quiver, v_quiver,
                      transform=ccrs.PlateCarree(), scale=200)
            ax2.scatter(turbine_lons, turbine_lats, c='red', s=20, marker='o', transform=ccrs.PlateCarree())
            ax2.set_title(f'10m Wind Speed & Direction - {date_str}')
        
        # 3. Surface pressure plot
        if pres_var:
            ax3 = plt.subplot(3, 2, 3, projection=ccrs.PlateCarree())
            pres = reduce_to_2d(ds[pres_var], ds)
            
            # Convert from Pa to hPa if needed
            if pres.max() > 10000:
                pres = pres / 100.0
            
            ax3.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
            ax3.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax3.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=':')
            ax3.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
            
            cf = ax3.contourf(lons, lats, pres, levels=15, cmap='coolwarm', transform=ccrs.PlateCarree())
            plt.colorbar(cf, ax=ax3, label='Pressure (hPa)', shrink=0.8)
            ax3.scatter(turbine_lons, turbine_lats, c='black', s=20, marker='o', transform=ccrs.PlateCarree())
            ax3.set_title(f'Surface Pressure - {date_str}')
        
        # 4. Precipitation plot
        if precip_var:
            ax4 = plt.subplot(3, 2, 4, projection=ccrs.PlateCarree())
            precip = reduce_to_2d(ds[precip_var], ds)
            
            ax4.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
            ax4.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax4.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=':')
            ax4.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
            
            # Use a precipitation-friendly colormap
            cf = ax4.contourf(lons, lats, precip, levels=15, cmap='Blues', transform=ccrs.PlateCarree())
            plt.colorbar(cf, ax=ax4, label='Precipitation (kg/m²)', shrink=0.8)
            ax4.scatter(turbine_lons, turbine_lats, c='red', s=20, marker='o', transform=ccrs.PlateCarree())
            ax4.set_title(f'Precipitation - {date_str}')
        else:
            ax4 = plt.subplot(3, 2, 4)
            ax4.text(0.5, 0.5, 'Precipitation data\nnot available', 
                    ha='center', va='center', fontsize=12)
            ax4.axis('off')
        
        # 5. Precipitable Water Vapor plot
        if pwv_var:
            ax5 = plt.subplot(3, 2, 5, projection=ccrs.PlateCarree())
            pwv = reduce_to_2d(ds[pwv_var], ds)
            
            ax5.set_extent([lons.min(), lons.max(), lats.min(), lats.max()], crs=ccrs.PlateCarree())
            ax5.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax5.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=':')
            ax5.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
            
            cf = ax5.contourf(lons, lats, pwv, levels=15, cmap='YlGnBu', transform=ccrs.PlateCarree())
            plt.colorbar(cf, ax=ax5, label='PWV (kg/m²)', shrink=0.8)
            ax5.scatter(turbine_lons, turbine_lats, c='red', s=20, marker='o', transform=ccrs.PlateCarree())
            ax5.set_title(f'Precipitable Water Vapor - {date_str}')
        else:
            ax5 = plt.subplot(3, 2, 5)
            ax5.text(0.5, 0.5, 'Precipitable Water Vapor\ndata not available', 
                    ha='center', va='center', fontsize=12)
            ax5.axis('off')
        
        # 6. Info panel
        ax6 = plt.subplot(3, 2, 6)
        info_text = f"""Date: {date_str}
Cycle: 06Z
Model: MEPS Deterministic 2.5km

Variables found:
  Temperature: {temp_var or 'N/A'}
  U-wind: {u_var or 'N/A'}
  V-wind: {v_var or 'N/A'}
  Pressure: {pres_var or 'N/A'}
  Precipitation: {precip_var or 'N/A'}
  PWV: {pwv_var or 'N/A'}"""
        
        ax6.text(0.1, 0.5, info_text, ha='left', va='center', fontsize=11,
                family='monospace', transform=ax6.transAxes)
        ax6.axis('off')
        
        plt.tight_layout()
        
        output_file = os.path.join(output_dir, f'weather_{date_str}.png')
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"Saved plot: {output_file}")
        
    except Exception as e:
        print(f"Error creating plot for {date_str}: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description='Download MET Norway MEPS 2.5km weather data and create plots',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a specific date
  python download_met_weather_meps25.py --date 20231115
  
  # List available variables for a date
  python download_met_weather_meps25.py --date 20231115 --list-vars
  
  # Process multiple dates from analysis plots
  python download_met_weather_meps25.py --n-plots 5
  
  # Use different forecast cycle
  python download_met_weather_meps25.py --date 20231115 --cycle 00
""")
    parser.add_argument('--date', type=str, default=None,
                        help='Specific date to process (YYYYMMDD format). If not provided, '
                             'uses dates from analysis plots.')
    parser.add_argument('--n-plots', type=int, default=10, 
                        help='Number of most interesting dates to process (default: 10)')
    parser.add_argument('--cycle', type=str, default='06', 
                        help='Forecast cycle (default: 06)')
    parser.add_argument('--buffer', type=float, default=1.5, 
                        help='Domain buffer in degrees (default: 1.5)')
    parser.add_argument('--analysis-dir', type=str, default='analysis_plots',
                        help='Directory containing *_drop_*.png plots (default: analysis_plots)')
    parser.add_argument('--output-dir', type=str, default='weather_data/meps25',
                        help='Directory to save downloaded files (default: weather_data/meps25)')
    parser.add_argument('--plot-dir', type=str, default='weather_plots/meps25',
                        help='Directory to save plots (default: weather_plots/meps25)')
    parser.add_argument('--list-vars', action='store_true',
                        help='Just list available variables and exit')
    args = parser.parse_args()
    
    print("=" * 60)
    print("MEPS 2.5km Weather Data Download and Plotting")
    print("(Using OPeNDAP for server-side subsetting)")
    print("=" * 60)
    
    # If just listing variables, do that and exit
    if args.list_vars:
        date_to_check = args.date or '20231115'  # Default date for variable listing
        print(f"\nListing variables for date: {date_to_check}")
        list_available_variables(date_to_check, cycle=args.cycle)
        return
    
    print("\nStep 1: Getting turbine domain...")
    domain, turbine_lats, turbine_lons = get_turbine_domain(buffer_deg=args.buffer)
    
    # Determine which dates to process
    if args.date:
        # Use the specific date provided
        selected_dates = [args.date]
        print(f"\nUsing specified date: {args.date}")
    else:
        # Use dates from analysis plots
        print("\nStep 2: Analyzing power drop events...")
        date_info_df, date_counts = get_interesting_dates(analysis_dir=args.analysis_dir)
        
        if date_counts.empty:
            print("\nNo drop-event plots found, so there are no dates to fetch/plot.")
            print("Tip: Use --date YYYYMMDD to specify a date directly, or")
            print("     run `power_analysis.py` first (it creates the *_drop_*.png plots), or")
            print("     pass `--analysis-dir processed_data/analysis_plots` if your plots live there.")
            return
        
        print("\nStep 3: Selecting most interesting dates...")
        selected_dates = select_most_interesting_dates(date_counts, n_dates=args.n_plots)
    
    print(f"\nStep 4: Fetching data and creating plots for {len(selected_dates)} date(s)...")
    print("(Data is subsetted on server - only downloading region around turbines)")
    
    for i, date_str in enumerate(selected_dates, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(selected_dates)}] Processing {date_str}...")
        print('='*60)
        
        # Fetch and subset data via OPeNDAP
        ds = fetch_and_subset_meps25(date_str, cycle=args.cycle, domain=domain, 
                                     output_dir=args.output_dir)
        
        if ds is not None:
            plot_weather_meps25(ds, date_str, turbine_lats, turbine_lons, 
                               output_dir=args.plot_dir)
            ds.close()
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
