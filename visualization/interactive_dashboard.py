import os
import sys
import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import xarray as xr
import duckdb
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# Add the project root to the Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

try:
    from config import DATABASE_PATH as DB_PATH_REL, ZARR_STORE_PATH as ZARR_PATH_REL, OUTPUT_FORMAT, EUROPE_BOUNDS
    # Make paths absolute
    DATABASE_PATH = os.path.join(PROJECT_ROOT, DB_PATH_REL)
    ZARR_STORE_PATH = os.path.join(PROJECT_ROOT, ZARR_PATH_REL)
except ImportError:
    # Fallback configuration if config file is not available
    DATABASE_PATH = os.path.join(PROJECT_ROOT, "data/processed/gfs_data.duckdb")
    ZARR_STORE_PATH = os.path.join(PROJECT_ROOT, "data/processed/gfs_data.zarr")
    OUTPUT_FORMAT = "duckdb"  # or "zarr"
    EUROPE_BOUNDS = {
        'lon_min': -10, 'lon_max': 30,
        'lat_min': 35, 'lat_max': 70
    }

class WeatherDataLoader:
    """Handles loading data from different sources and formats"""
    
    def __init__(self):
        self.gfs_variables = {
            'wind_speed': {'label': 'Wind Speed', 'unit': 'm/s', 'colorscale': 'Viridis'},
            'wind_direction': {'label': 'Wind Direction', 'unit': 'degrees', 'colorscale': 'HSV'},
            'u_wind': {'label': 'U-Wind Component', 'unit': 'm/s', 'colorscale': 'RdBu'},
            'v_wind': {'label': 'V-Wind Component', 'unit': 'm/s', 'colorscale': 'RdBu'},
            'temperature': {'label': 'Temperature', 'unit': 'K', 'colorscale': 'RdYlBu_r'},
            'precipitation': {'label': 'Precipitation', 'unit': 'kg/m²', 'colorscale': 'Blues'},
            'cloud_cover': {'label': 'Cloud Cover', 'unit': 'fraction', 'colorscale': 'Greys'},
            'precipitable_water': {'label': 'Precipitable Water', 'unit': 'kg/m²', 'colorscale': 'Blues'},
            'mean_sea_level_pressure': {'label': 'Sea Level Pressure', 'unit': 'Pa', 'colorscale': 'RdBu_r'}
        }
        
        self.meps_variables = {
            'air_temperature_2m': {'label': 'Air Temperature (2m)', 'unit': 'K', 'colorscale': 'RdYlBu_r'},
            'precipitation_amount': {'label': 'Precipitation Amount', 'unit': 'kg/m²', 'colorscale': 'Blues'},
            'wind_direction_10m': {'label': 'Wind Direction (10m)', 'unit': 'degrees', 'colorscale': 'HSV'},
            'wind_speed_10m': {'label': 'Wind Speed (10m)', 'unit': 'm/s', 'colorscale': 'Viridis'},
            'wind_speed_of_gust': {'label': 'Wind Gust Speed', 'unit': 'm/s', 'colorscale': 'Viridis'},
            'cloud_area_fraction': {'label': 'Cloud Area Fraction', 'unit': 'fraction', 'colorscale': 'Greys'},
            'air_pressure_at_sea_level': {'label': 'Sea Level Pressure', 'unit': 'Pa', 'colorscale': 'RdBu_r'},
            'relative_humidity_2m': {'label': 'Relative Humidity (2m)', 'unit': 'fraction', 'colorscale': 'Blues'},
            'probability_of_snow_phase': {'label': 'Snow Probability', 'unit': 'fraction', 'colorscale': 'Blues'},
            'integral_of_surface_downwelling_longwave_flux_in_air_wrt_time': {
                'label': 'Longwave Radiation', 'unit': 'J/m²', 'colorscale': 'Oranges'
            },
            'integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time': {
                'label': 'Shortwave Radiation', 'unit': 'W s/m²', 'colorscale': 'YlOrRd'
            }
        }
    
    def get_available_datasets(self):
        """Get list of available datasets"""
        datasets = []
        
        # Check GFS data
        if OUTPUT_FORMAT == "duckdb" and os.path.exists(DATABASE_PATH):
            datasets.append("GFS (Global)")
        elif OUTPUT_FORMAT == "zarr" and os.path.exists(ZARR_STORE_PATH):
            datasets.append("GFS (Global)")
            
        # Check MEPS data
        meps_path = os.path.join(PROJECT_ROOT, "data/raw/met")
        if os.path.exists(meps_path):
            datasets.append("MEPS (Nordic High-Res)")
            
        return datasets
    
    def get_available_times(self, dataset):
        """Get available times for a dataset"""
        if dataset == "GFS (Global)":
            return self._get_gfs_times()
        elif dataset == "MEPS (Nordic High-Res)":
            return self._get_meps_times()
        return []
    
    def _get_gfs_times(self):
        """Get available times from GFS data"""
        try:
            if OUTPUT_FORMAT == "duckdb":
                conn = duckdb.connect(DATABASE_PATH)
                times = conn.execute("SELECT DISTINCT time FROM gfs_data ORDER BY time DESC LIMIT 50").fetchdf()
                conn.close()
                return times['time'].dt.strftime('%Y-%m-%d %H:%M').tolist()
            elif OUTPUT_FORMAT == "zarr":
                ds = xr.open_zarr(ZARR_STORE_PATH)
                return pd.to_datetime(ds.time.values).strftime('%Y-%m-%d %H:%M').tolist()
        except Exception as e:
            print(f"Error getting GFS times: {e}")
            return []
    
    def _get_meps_times(self):
        """Get available times from MEPS data"""
        times = []
        meps_path = os.path.join(PROJECT_ROOT, "data/raw/met")
        
        if os.path.exists(meps_path):
            for date_dir in sorted(os.listdir(meps_path))[-10:]:  # Last 10 days
                date_path = os.path.join(meps_path, date_dir)
                if os.path.isdir(date_path):
                    for cycle_dir in os.listdir(date_path):
                        cycle_path = os.path.join(date_path, cycle_dir)
                        if os.path.isdir(cycle_path):
                            # Check if there are NetCDF files
                            nc_files = [f for f in os.listdir(cycle_path) if f.endswith('.nc')]
                            if nc_files:
                                try:
                                    datetime_obj = datetime.strptime(f"{date_dir} {cycle_dir}", "%Y%m%d %H")
                                    times.append(datetime_obj.strftime('%Y-%m-%d %H:%M'))
                                except ValueError:
                                    continue
        
        return sorted(times, reverse=True)
    
    def load_data(self, dataset, time_str, variable):
        """Load data for visualization"""
        if dataset == "GFS (Global)":
            return self._load_gfs_data(time_str, variable)
        elif dataset == "MEPS (Nordic High-Res)":
            return self._load_meps_data(time_str, variable)
        return None
    
    def _load_gfs_data(self, time_str, variable):
        """Load GFS data"""
        try:
            if OUTPUT_FORMAT == "duckdb":
                conn = duckdb.connect(DATABASE_PATH)
                query = f"SELECT longitude, latitude, {variable} FROM gfs_data WHERE time = '{time_str}'"
                df = conn.execute(query).fetchdf()
                conn.close()
            elif OUTPUT_FORMAT == "zarr":
                ds = xr.open_zarr(ZARR_STORE_PATH)
                time_idx = pd.to_datetime(ds.time.values).strftime('%Y-%m-%d %H:%M').tolist().index(time_str)
                data_slice = ds.isel(time=time_idx)
                df = data_slice[[variable, 'longitude', 'latitude']].to_dataframe().reset_index()
            
            return df.dropna()
            
        except Exception as e:
            print(f"Error loading GFS data: {e}")
            return pd.DataFrame()
    
    def _load_meps_data(self, time_str, variable):
        """Load MEPS data from NetCDF files"""
        try:
            # Parse time string to find the file
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M')
            date_str = dt.strftime('%Y%m%d')
            cycle_str = dt.strftime('%H')
            
            meps_dir = os.path.join(PROJECT_ROOT, "data", "raw", "met", date_str, cycle_str)
            
            if not os.path.exists(meps_dir):
                return pd.DataFrame()
            
            # Find the NetCDF file
            nc_files = [f for f in os.listdir(meps_dir) if f.endswith('.nc')]
            if not nc_files:
                return pd.DataFrame()
            
            nc_path = os.path.join(meps_dir, nc_files[0])
            
            # Load the NetCDF file
            ds = xr.open_dataset(nc_path)
            
            if variable not in ds.variables:
                return pd.DataFrame()
            
            # Get the first time step if multiple exist
            if 'time' in ds[variable].dims:
                data_var = ds[variable].isel(time=0)
            else:
                data_var = ds[variable]
            
            # Convert to DataFrame
            df = data_var.to_dataframe().reset_index()
            df = df[['longitude', 'latitude', variable]].dropna()
            
            ds.close()
            return df
            
        except Exception as e:
            print(f"Error loading MEPS data: {e}")
            return pd.DataFrame()
    
    def get_variable_info(self, dataset, variable):
        """Get variable metadata"""
        if dataset == "GFS (Global)":
            return self.gfs_variables.get(variable, {})
        elif dataset == "MEPS (Nordic High-Res)":
            return self.meps_variables.get(variable, {})
        return {}

# Initialize the data loader
data_loader = WeatherDataLoader()

# Initialize Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Interactive Weather Data Dashboard", style={'textAlign': 'center', 'marginBottom': 30}),
    
    html.Div([
        html.Div([
            html.Label("Dataset:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='dataset-dropdown',
                options=[{'label': ds, 'value': ds} for ds in data_loader.get_available_datasets()],
                value=data_loader.get_available_datasets()[0] if data_loader.get_available_datasets() else None,
                style={'marginBottom': 10}
            ),
        ], className="three columns"),
        
        html.Div([
            html.Label("Time:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='time-dropdown',
                style={'marginBottom': 10}
            ),
        ], className="three columns"),
        
        html.Div([
            html.Label("Variable:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='variable-dropdown',
                style={'marginBottom': 10}
            ),
        ], className="three columns"),
        
        html.Div([
            html.Label("Plot Type:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='plot-type-dropdown',
                options=[
                    {'label': 'Filled Contour', 'value': 'contour'},
                    {'label': 'Scatter Points', 'value': 'scatter'},
                    {'label': 'Heatmap', 'value': 'heatmap'}
                ],
                value='contour',
                style={'marginBottom': 10}
            ),
        ], className="three columns"),
    ], className="row", style={'margin': '20px'}),
    
    html.Div([
        dcc.Loading(
            id="loading",
            children=[dcc.Graph(id='weather-map')],
            type="default",
        )
    ]),
    
    html.Div([
        html.H3("Data Statistics", style={'textAlign': 'center'}),
        html.Div(id='data-stats', style={'textAlign': 'center', 'marginTop': 20})
    ])
], style={'fontFamily': 'Arial, sans-serif'})

@callback(
    Output('time-dropdown', 'options'),
    Output('time-dropdown', 'value'),
    Input('dataset-dropdown', 'value')
)
def update_time_dropdown(selected_dataset):
    if not selected_dataset:
        return [], None
    
    times = data_loader.get_available_times(selected_dataset)
    options = [{'label': t, 'value': t} for t in times]
    value = times[0] if times else None
    
    return options, value

@callback(
    Output('variable-dropdown', 'options'),
    Output('variable-dropdown', 'value'),
    Input('dataset-dropdown', 'value')
)
def update_variable_dropdown(selected_dataset):
    if not selected_dataset:
        return [], None
    
    if selected_dataset == "GFS (Global)":
        variables = data_loader.gfs_variables
    elif selected_dataset == "MEPS (Nordic High-Res)":
        variables = data_loader.meps_variables
    else:
        return [], None
    
    options = [{'label': info['label'], 'value': var} for var, info in variables.items()]
    value = list(variables.keys())[0] if variables else None
    
    return options, value

@callback(
    Output('weather-map', 'figure'),
    Output('data-stats', 'children'),
    Input('dataset-dropdown', 'value'),
    Input('time-dropdown', 'value'),
    Input('variable-dropdown', 'value'),
    Input('plot-type-dropdown', 'value')
)
def update_map(selected_dataset, selected_time, selected_variable, plot_type):
    if not all([selected_dataset, selected_time, selected_variable]):
        return {}, "Please select dataset, time, and variable."
    
    # Load data
    df = data_loader.load_data(selected_dataset, selected_time, selected_variable)
    
    if df.empty:
        return {}, "No data available for the selected parameters."
    
    # Get variable information
    var_info = data_loader.get_variable_info(selected_dataset, selected_variable)
    
    # Create the plot
    fig = go.Figure()
    
    if plot_type == 'contour':
        # Create contour plot
        fig.add_trace(go.Scattermapbox(
            lon=df['longitude'],
            lat=df['latitude'],
            mode='markers',
            marker=dict(
                size=3,
                color=df[selected_variable],
                colorscale=var_info.get('colorscale', 'Viridis'),
                colorbar=dict(title=f"{var_info.get('label', selected_variable)} ({var_info.get('unit', '')})"),
                cmin=df[selected_variable].quantile(0.05),
                cmax=df[selected_variable].quantile(0.95)
            ),
            text=[f"Lon: {lon:.2f}<br>Lat: {lat:.2f}<br>{var_info.get('label', selected_variable)}: {val:.2f} {var_info.get('unit', '')}"
                  for lon, lat, val in zip(df['longitude'], df['latitude'], df[selected_variable])],
            hovertemplate='%{text}<extra></extra>'
        ))
    
    elif plot_type == 'scatter':
        fig.add_trace(go.Scattermapbox(
            lon=df['longitude'],
            lat=df['latitude'],
            mode='markers',
            marker=dict(
                size=5,
                color=df[selected_variable],
                colorscale=var_info.get('colorscale', 'Viridis'),
                colorbar=dict(title=f"{var_info.get('label', selected_variable)} ({var_info.get('unit', '')})"),
            ),
            text=[f"Lon: {lon:.2f}<br>Lat: {lat:.2f}<br>{var_info.get('label', selected_variable)}: {val:.2f} {var_info.get('unit', '')}"
                  for lon, lat, val in zip(df['longitude'], df['latitude'], df[selected_variable])],
            hovertemplate='%{text}<extra></extra>'
        ))
    
    elif plot_type == 'heatmap':
        # For heatmap, we need to interpolate to a regular grid
        try:
            from scipy.interpolate import griddata
            
            # Create regular grid
            lon_min, lon_max = df['longitude'].min(), df['longitude'].max()
            lat_min, lat_max = df['latitude'].min(), df['latitude'].max()
            
            grid_lon = np.linspace(lon_min, lon_max, 100)
            grid_lat = np.linspace(lat_min, lat_max, 100)
            grid_lon_2d, grid_lat_2d = np.meshgrid(grid_lon, grid_lat)
            
            # Interpolate data to grid
            points = np.column_stack((df['longitude'].values, df['latitude'].values))
            values = df[selected_variable].values
            grid_values = griddata(points, values, (grid_lon_2d, grid_lat_2d), method='linear')
            
            fig.add_trace(go.Densitymapbox(
                lon=grid_lon_2d.flatten(),
                lat=grid_lat_2d.flatten(),
                z=grid_values.flatten(),
                colorscale=var_info.get('colorscale', 'Viridis'),
                colorbar=dict(title=f"{var_info.get('label', selected_variable)} ({var_info.get('unit', '')})"),
                radius=20
            ))
        except ImportError:
            # Fallback to scatter if scipy is not available
            fig.add_trace(go.Scattermapbox(
                lon=df['longitude'],
                lat=df['latitude'],
                mode='markers',
                marker=dict(
                    size=5,
                    color=df[selected_variable],
                    colorscale=var_info.get('colorscale', 'Viridis'),
                    colorbar=dict(title=f"{var_info.get('label', selected_variable)} ({var_info.get('unit', '')})"),
                ),
            ))
    
    # Set map layout
    bounds = EUROPE_BOUNDS
    if selected_dataset == "MEPS (Nordic High-Res)" and not df.empty:
        # Adjust bounds for Nordic data
        bounds = {
            'lon_min': df['longitude'].min() - 1,
            'lon_max': df['longitude'].max() + 1,
            'lat_min': df['latitude'].min() - 1,
            'lat_max': df['latitude'].max() + 1
        }
    
    fig.update_layout(
        mapbox=dict(
            style='open-street-map',
            center=dict(
                lon=(bounds['lon_min'] + bounds['lon_max']) / 2,
                lat=(bounds['lat_min'] + bounds['lat_max']) / 2
            ),
            zoom=3 if selected_dataset == "GFS (Global)" else 5
        ),
        title=f"{var_info.get('label', selected_variable)} - {selected_dataset} - {selected_time}",
        height=600
    )
    
    # Calculate statistics
    stats_text = [
        html.P(f"Data Points: {len(df):,}"),
        html.P(f"Min: {df[selected_variable].min():.2f} {var_info.get('unit', '')}"),
        html.P(f"Max: {df[selected_variable].max():.2f} {var_info.get('unit', '')}"),
        html.P(f"Mean: {df[selected_variable].mean():.2f} {var_info.get('unit', '')}"),
        html.P(f"Std: {df[selected_variable].std():.2f} {var_info.get('unit', '')}")
    ]
    
    return fig, stats_text

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
