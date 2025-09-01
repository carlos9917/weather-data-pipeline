
import xarray as xr
import numpy as np

def calculate_wind_gust(zarr_path, gust_factor=1.5):
    """
    Calculates wind gust from GFS data stored in a Zarr dataset.

    Args:
        zarr_path (str): The path to the Zarr dataset.
        gust_factor (float): The factor to multiply the wind speed by to estimate gust.

    Returns:
        xarray.DataArray: A DataArray containing the calculated wind gust.
    """
    # Open the Zarr dataset
    try:
        gfs_data = xr.open_zarr(zarr_path)
    except Exception as e:
        print(f"Error opening Zarr dataset at {zarr_path}: {e}")
        return None

    # Extract the 10m wind components
    u_wind_10m = gfs_data['u_wind_10m']
    v_wind_10m = gfs_data['v_wind_10m']

    # Calculate the 10m wind speed
    wind_speed_10m = np.sqrt(u_wind_10m**2 + v_wind_10m**2)

    # Calculate the wind gust
    wind_gust = wind_speed_10m * gust_factor
    wind_gust.name = 'wind_gust_10m'
    wind_gust.attrs['long_name'] = 'Wind gust at 10m'
    wind_gust.attrs['units'] = 'm s**-1'
    wind_gust.attrs['gust_factor'] = str(gust_factor)


    return wind_gust

if __name__ == '__main__':
    # The user mentioned the zarr data is in a path like data/processed/gfs_data.zarr
    # Note: This path might need to be adjusted depending on where the script is run from.
    zarr_store_path = '/home/tenantadmin/weather-data-pipeline/data/processed/gfs_data.zarr'
    
    print(f"Calculating wind gust from: {zarr_store_path}")
    
    wind_gust_data = calculate_wind_gust(zarr_store_path)

    if wind_gust_data is not None:
        print("\nSuccessfully calculated wind gust.")
        print("Here's a sample of the data (first time step, first latitude, first 5 longitudes):")
        print(wind_gust_data.isel(time=0, latitude=0, longitude=slice(0, 5)).values)
