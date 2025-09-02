import os
import sys
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import xarray as xr
import numpy as np

# Add the project root to the Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

try:
    from config import ZARR_STORE_PATH as ZARR_PATH_REL, EUROPE_BOUNDS
    ZARR_STORE_PATH = os.path.join(PROJECT_ROOT, ZARR_PATH_REL)
except ImportError:
    ZARR_STORE_PATH = os.path.join(PROJECT_ROOT, "data/processed/gfs_data.zarr")
    EUROPE_BOUNDS = {'lon_min': -10, 'lon_max': 30, 'lat_min': 35, 'lat_max': 70}

# --- Data Loading and Configuration ---
def get_dataset():
    """Loads the Zarr dataset and returns it, caching if possible."""
    if 'dataset' not in get_dataset.__dict__:
        if not os.path.exists(ZARR_STORE_PATH):
            print(f"Error: Zarr store not found at {ZARR_STORE_PATH}")
            return None
        get_dataset.dataset = xr.open_zarr(ZARR_STORE_PATH)
    return get_dataset.dataset

ds = get_dataset()

# --- App Initialization and Data Prep ---
if ds is not None:
    # Get available initialization times
    init_times = pd.to_datetime(ds.init_time.values)
    init_time_options = [{'label': t.strftime('%Y-%m-%d %H:%M UTC'), 'value': str(t)} for t in sorted(init_times, reverse=True)]
    default_init_time = str(sorted(init_times, reverse=True)[0]) if init_time_options else None
else:
    init_time_options = []
    default_init_time = None

VARIABLE_CONFIG = {
    'temperature': {'label': 'Temperature (2m)', 'unit': '°C', 'colorscale': 'RdYlBu_r', 'convert': lambda x: x - 273.15},
    'precipitation_rate': {'label': 'Precipitation Rate', 'unit': 'mm/hr', 'colorscale': 'Blues', 'convert': lambda x: x},
    'cloud_cover': {'label': 'Cloud Cover', 'unit': '%', 'colorscale': 'Greys_r', 'convert': lambda x: x},
    'surface_pressure': {'label': 'Surface Pressure', 'unit': 'hPa', 'colorscale': 'Viridis', 'convert': lambda x: x / 100},
    'wind_speed_10m': {'label': 'Wind Speed (10m)', 'unit': 'm/s', 'colorscale': 'Viridis', 'convert': lambda x: x},
    'wind_speed_100m': {'label': 'Wind Speed (100m)', 'unit': 'm/s', 'colorscale': 'Plasma', 'convert': lambda x: x},
    'wind_gust': {'label': 'Wind Gust (Surface)', 'unit': 'm/s', 'colorscale': 'YlOrRd', 'convert': lambda x: x},
}

# --- Dash App Initialization ---
app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])
app.title = "Weather Forecast Dashboard"

# --- App Layout ---
app.layout = html.Div([
    html.H1("Interactive Weather Forecast Dashboard", style={'textAlign': 'center'}),
    
    html.Div(className='row', children=[
        html.Div(className='three columns', children=[
            html.Label("Select GFS Cycle (Init Time):"),
            dcc.Dropdown(
                id='init-time-dropdown',
                options=init_time_options,
                value=default_init_time
            ),
        ]),
        html.Div(className='three columns', children=[
            html.Label("Select Weather Variable:"),
            dcc.Dropdown(
                id='variable-dropdown',
                # Options will be populated by callback
            ),
        ]),
        html.Div(className='six columns', children=[
            html.Label("Select Forecast Hour:"),
            dcc.Slider(
                id='time-slider',
                min=0,
                # Max and marks will be populated by callback
            ),
            html.Div(id='slider-output-container', style={'textAlign': 'center', 'marginTop': '10px'})
        ]),
    ]),
    
    html.Hr(),
    
    html.Div(className='row', children=[
        html.Div(className='seven columns', children=[
            dcc.Loading(dcc.Graph(id='weather-map'))
        ]),
        html.Div(className='five columns', children=[
            dcc.Loading(dcc.Graph(id='timeseries-plot'))
        ]),
    ]),
    dcc.Store(id='selected-point-store', data={'lat': 52.52, 'lon': 13.40}) # Default to Berlin
])

# --- Callbacks ---

@app.callback(
    Output('variable-dropdown', 'options'),
    Output('variable-dropdown', 'value'),
    Input('init-time-dropdown', 'value'))
def update_variable_dropdown(init_time):
    """Populates variables available for the selected GFS cycle."""
    if ds is None or init_time is None:
        return [], None
    
    ds_cycle = ds.sel(init_time=init_time)
    available_vars = [k for k, v in VARIABLE_CONFIG.items() if k in ds_cycle.variables]
    options = [{'label': VARIABLE_CONFIG[k]['label'], 'value': k} for k in available_vars]
    
    # Set a default value
    default_var = 'temperature' if 'temperature' in available_vars else available_vars[0] if available_vars else None
    return options, default_var

@app.callback(
    Output('time-slider', 'max'),
    Output('time-slider', 'marks'),
    Output('time-slider', 'value'),
    Input('init-time-dropdown', 'value'))
def update_time_slider(init_time):
    """Updates the time slider based on the selected GFS cycle."""
    if ds is None or init_time is None:
        return 0, {}, 0
        
    ds_cycle = ds.sel(init_time=init_time)
    time_coords = ds_cycle.time.values
    
    max_val = len(time_coords) - 1
    
    # Create marks for every 3 hours
    marks = {}
    for i in range(0, len(time_coords), 3):
        # Calculate forecast hour relative to init_time
        forecast_hour = (pd.to_datetime(time_coords[i]) - pd.to_datetime(init_time)).total_seconds() / 3600
        marks[i] = f"{int(forecast_hour)}h"

    return max_val, marks, 0

@app.callback(
    Output('slider-output-container', 'children'),
    Input('time-slider', 'value'),
    Input('init-time-dropdown', 'value'))
def update_slider_output(value, init_time):
    if ds is None or init_time is None: return "No data"
    
    ds_cycle = ds.sel(init_time=init_time)
    selected_time = pd.to_datetime(ds_cycle.time.values[value])
    return f"Forecast Valid Time: {selected_time.strftime('%Y-%m-%d %H:%M')} UTC"

@app.callback(
    Output('weather-map', 'figure'),
    Input('variable-dropdown', 'value'),
    Input('time-slider', 'value'),
    Input('init-time-dropdown', 'value'))
def update_map(selected_variable, time_index, init_time):
    if ds is None or selected_variable is None or init_time is None:
        return go.Figure()

    ds_cycle = ds.sel(init_time=init_time)
    var_config = VARIABLE_CONFIG[selected_variable]
    data_slice = ds_cycle[selected_variable].isel(time=time_index)
    
    # Apply conversion function (e.g., K to C)
    converted_data = var_config['convert'](data_slice.values)

    # Densitymapbox expects 1D arrays for lat, lon, and z.
    # We need to create a meshgrid and then flatten it.
    lon_grid, lat_grid = np.meshgrid(ds.longitude.values, ds.latitude.values)
    lon_flat = lon_grid.flatten()
    lat_flat = lat_grid.flatten()
    z_flat = converted_data.flatten()

    fig = go.Figure(go.Densitymapbox(
        lon=lon_flat,
        lat=lat_flat,
        z=z_flat,
        colorscale=var_config['colorscale'],
        colorbar_title=var_config['unit'],
        opacity=0.7,
        radius=20
    ))

    fig.update_layout(
        title=f"{var_config['label']} at {pd.to_datetime(ds_cycle.time.values[time_index]).strftime('%Y-%m-%d %H:%M')} UTC",
        mapbox_style="open-street-map",
        mapbox_center_lon=(EUROPE_BOUNDS['lon_min'] + EUROPE_BOUNDS['lon_max']) / 2,
        mapbox_center_lat=(EUROPE_BOUNDS['lat_min'] + EUROPE_BOUNDS['lat_max']) / 2,
        mapbox_zoom=3.5,
        margin={"r":0,"t":40,"l":0,"b":0}
    )
    return fig

@app.callback(
    Output('selected-point-store', 'data'),
    Input('weather-map', 'clickData'),
    State('selected-point-store', 'data'))
def store_clicked_point(clickData, current_point):
    if clickData:
        point = clickData['points'][0]
        return {'lat': point['lat'], 'lon': point['lon']}
    return current_point

@app.callback(
    Output('timeseries-plot', 'figure'),
    Input('variable-dropdown', 'value'),
    Input('selected-point-store', 'data'),
    Input('init-time-dropdown', 'value'))
def update_timeseries(selected_variable, selected_point, init_time):
    if ds is None or selected_variable is None or selected_point is None or init_time is None:
        return go.Figure()

    lat = selected_point['lat']
    lon = selected_point['lon']
    
    ds_cycle = ds.sel(init_time=init_time)
    
    # Find nearest point in dataset
    point_data = ds_cycle.sel(latitude=lat, longitude=lon, method='nearest')
    
    var_config = VARIABLE_CONFIG[selected_variable]
    timeseries_data = var_config['convert'](point_data[selected_variable].values)
    
    # Use the actual time values for the selected cycle
    time_values = pd.to_datetime(ds_cycle.time.values)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=time_values,
        y=timeseries_data,
        mode='lines+markers',
        name=var_config['label']
    ))
    
    fig.update_layout(
        title=f"Forecast for {var_config['label']}<br>Lat: {lat:.2f}, Lon: {lon:.2f}",
        xaxis_title="Time (UTC)",
        yaxis_title=f"{var_config['label']} ({var_config['unit']})",
        margin={"r":20,"t":80,"l":20,"b":20}
    )
    return fig

if __name__ == '__main__':
    if ds is None:
        print("Could not load data. Dashboard cannot start.")
    else:
        app.run_server(debug=True, host='0.0.0.0', port=8050)