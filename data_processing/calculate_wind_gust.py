import xarray as xr
import numpy as np
import argparse

def calculate_wind_gust_factor(gfs_data, gust_factor=1.5):
    """
    Calculates wind gust using a simple multiplicative factor.
    """
    u_wind_10m = gfs_data['u_wind_10m']
    v_wind_10m = gfs_data['v_wind_10m']
    wind_speed_10m = np.sqrt(u_wind_10m**2 + v_wind_10m**2)
    wind_gust = wind_speed_10m * gust_factor
    wind_gust.name = 'wind_gust_factor'
    wind_gust.attrs['long_name'] = f'Wind gust (factor method, factor={gust_factor})'
    wind_gust.attrs['units'] = 'm s**-1'
    return wind_gust

def calculate_wind_gust_friction_velocity(gfs_data, alpha=3.0):
    """
    Calculates wind gust using friction velocity (u*).
    Gust = U10m + alpha * u*
    """
    if 'u_flux' not in gfs_data or 'v_flux' not in gfs_data:
        print("Friction velocity variables (u_flux, v_flux) not found in dataset.")
        return None

    u_wind_10m = gfs_data['u_wind_10m']
    v_wind_10m = gfs_data['v_wind_10m']
    wind_speed_10m = np.sqrt(u_wind_10m**2 + v_wind_10m**2)

    # Friction velocity (u*) is the fourth root of the sum of squares of the momentum fluxes
    # This is a simplification. The density should be used.
    # u* = ( (u_flux^2 + v_flux^2)^(1/2) ) ^ (1/2) = (u_flux^2 + v_flux^2)^(1/4)
    u_star = (gfs_data['u_flux']**2 + gfs_data['v_flux']**2)**0.25
    
    wind_gust = wind_speed_10m + alpha * u_star
    wind_gust.name = 'wind_gust_friction_velocity'
    wind_gust.attrs['long_name'] = f'Wind gust (friction velocity method, alpha={alpha})'
    wind_gust.attrs['units'] = 'm s**-1'
    return wind_gust

def calculate_wind_gust_tke(gfs_data, beta=2.0):
    """
    Calculates wind gust using Turbulent Kinetic Energy (TKE).
    Gust = U10m + beta * sqrt(TKE)
    """
    if 'tke_10m' not in gfs_data:
        print("TKE variable (tke_10m) not found in dataset.")
        return None

    u_wind_10m = gfs_data['u_wind_10m']
    v_wind_10m = gfs_data['v_wind_10m']
    wind_speed_10m = np.sqrt(u_wind_10m**2 + v_wind_10m**2)
    
    wind_gust = wind_speed_10m + beta * np.sqrt(gfs_data['tke_10m'])
    wind_gust.name = 'wind_gust_tke'
    wind_gust.attrs['long_name'] = f'Wind gust (TKE method, beta={beta})'
    wind_gust.attrs['units'] = 'm s**-1'
    return wind_gust

def main(zarr_path):
    """
    Main function to calculate and compare wind gust methods.
    """
    try:
        gfs_data = xr.open_zarr(zarr_path)
    except Exception as e:
        print(f"Error opening Zarr dataset at {zarr_path}: {e}")
        return

    print(f"Calculating wind gusts from: {zarr_path}")

    # --- Method 1: Multiplicative Factor ---
    wind_gust_factor = calculate_wind_gust_factor(gfs_data)
    print("\n--- Multiplicative Factor Method ---")
    print("Sample values:", wind_gust_factor.isel(time=0, latitude=0, longitude=slice(0, 5)).values)

    # --- Method 2: Friction Velocity ---
    wind_gust_friction = calculate_wind_gust_friction_velocity(gfs_data)
    if wind_gust_friction is not None:
        print("\n--- Friction Velocity Method ---")
        print("Sample values:", wind_gust_friction.isel(time=0, latitude=0, longitude=slice(0, 5)).values)

    # --- Method 3: TKE ---
    wind_gust_tke = calculate_wind_gust_tke(gfs_data)
    if wind_gust_tke is not None:
        print("\n--- TKE Method ---")
        print("Sample values:", wind_gust_tke.isel(time=0, latitude=0, longitude=slice(0, 5)).values)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Calculate wind gust using different methods.")
    parser.add_argument("--zarr_path", default='data/processed/gfs_20250903_06.zarr', help="Path to the GFS Zarr dataset.")
    args = parser.parse_args()
    
    main(args.zarr_path)
