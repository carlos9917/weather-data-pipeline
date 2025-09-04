import os
import sys
from pathlib import Path
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import xarray as xr
import numpy as np

# Add the project root to the Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

try:
    from config import EUROPE_BOUNDS
except ImportError:
    EUROPE_BOUNDS = {'lon_min': -10, 'lon_max': 30, 'lat_min': 35, 'lat_max': 70}

# --- Data Loading and Configuration ---
def get_available_cycles():
    """Scans the processed data directory for available GFS and MET cycles."""
    processed_dir = PROJECT_ROOT / "data" / "processed"
    cycles = {'GFS': [], 'MET': []}
    if not processed_dir.exists():
        return cycles

    for f in processed_dir.glob('*.zarr'):
        stem = f.stem
        model_key, model_val, date, cycle_str = None, None, None, None

        if stem.startswith('gfs_'):
            parts = stem.split('_')
            if len(parts) == 3:
                model_val, date, cycle_str = parts
                model_key = 'GFS'
        elif stem.startswith('met_data_'):
            parts = stem.split('_')
            if len(parts) == 4:
                model_val = f"{parts[0]}_{parts[1]}"
                date = parts[2]
                cycle_str = parts[3]
                model_key = 'MET'

        if all((model_key, model_val, date, cycle_str)):
            label = f"{date[:4]}-{date[4:6]}-{date[6:]} {cycle_str}Z ({model_key})"
            value = f"{model_val.upper()}|{date}|{cycle_str}"
            cycles[model_key].append({'label': label, 'value': value})

    # Sort by label descending
    for model in cycles:
        cycles[model] = sorted(cycles[model], key=lambda x: x['label'], reverse=True)
        
    return cycles

AVAILABLE_CYCLES = get_available_cycles()

def load_dataset(cycle_value):
    """Loads a specific Zarr dataset based on the cycle dropdown value."""
    if not cycle_value:
        return None
    
    model, date, cycle = cycle_value.split('|')
    file_name = f"{model.lower()}_{date}_{cycle}.zarr"
    zarr_path = PROJECT_ROOT / "data" / "processed" / file_name
    
    if not zarr_path.exists():
        print(f"Error: Zarr store not found at {zarr_path}")
        return None
    
    try:
        ds = xr.open_zarr(zarr_path)
        # Select the first init_time if the dimension exists
        if 'init_time' in ds.dims:
            ds = ds.isel(init_time=0)
        return ds
    except Exception as e:
        print(f"Error loading Zarr store {zarr_path}: {e}")
        return None

VARIABLE_CONFIG = {
    'temperature': {'label': 'Temperature (2m)', 'unit': '°C', 'colorscale': 'RdYlBu_r', 'convert': lambda x: x - 273.15},
    'air_temperature_2m': {'label': 'Temperature (2m)', 'unit': '°C', 'colorscale': 'RdYlBu_r', 'convert': lambda x: x - 273.15},
    'precipitation_rate': {'label': 'Precipitation Rate', 'unit': 'mm/hr', 'colorscale': 'Blues', 'convert': lambda x: x},
    'precipitation_amount': {'label': 'Precipitation Amount', 'unit': 'kg/m^2', 'colorscale': 'Blues', 'convert': lambda x: x},
    'cloud_cover': {'label': 'Cloud Cover', 'unit': '%', 'colorscale': 'Greys_r', 'convert': lambda x: x * 100},
    'cloud_area_fraction': {'label': 'Cloud Cover', 'unit': '%', 'colorscale': 'Greys_r', 'convert': lambda x: x * 100},
    'surface_pressure': {'label': 'Surface Pressure', 'unit': 'hPa', 'colorscale': 'Viridis', 'convert': lambda x: x / 100},
    'air_pressure_at_sea_level': {'label': 'Surface Pressure', 'unit': 'hPa', 'colorscale': 'Viridis', 'convert': lambda x: x / 100},
    'wind_speed_10m': {'label': 'Wind Speed (10m)', 'unit': 'm/s', 'colorscale': 'Viridis', 'convert': lambda x: x},
    'wind_speed_100m': {'label': 'Wind Speed (100m)', 'unit': 'm/s', 'colorscale': 'Plasma', 'convert': lambda x: x},
    'wind_gust': {'label': 'Wind Gust (Surface)', 'unit': 'm/s', 'colorscale': 'YlOrRd', 'convert': lambda x: x},
}

# --- Dash App Initialization ---
app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'], suppress_callback_exceptions=True)
app.title = "Weather Forecast Dashboard"

# --- App Layout ---
app.layout = html.Div([
    html.H1("Interactive Weather Forecast Dashboard", style={'textAlign': 'center'}),
    
    html.Div(className='row', children=[
        html.Div(className='four columns', children=[
            html.Label("Select Forecast Cycle:"),
            dcc.Dropdown(
                id='cycle-dropdown',
                options=AVAILABLE_CYCLES.get('GFS', []) + AVAILABLE_CYCLES.get('MET', []),
                value=(AVAILABLE_CYCLES.get('GFS') + AVAILABLE_CYCLES.get('MET', []))[0]['value'] if (AVAILABLE_CYCLES.get('GFS') + AVAILABLE_CYCLES.get('MET', [])) else None
            ),
        ]),
        html.Div(className='four columns', children=[
            html.Label("Select Weather Variable:"),
            dcc.Dropdown(id='variable-dropdown'),
        ]),
    ]),
    
    html.Hr(),
    
    html.Div(className='row', children=[
        html.Div(className='seven columns', children=[
            dcc.Loading(dcc.Graph(id='weather-map')),
            html.Div([
                html.Label("Select Time:"),
                dcc.Slider(
                    id='time-slider',
                    min=0,
                    max=10,  # Will be updated dynamically
                    value=0,
                    marks={},
                    step=1
                )
            ], id='time-slider-container', style={'marginTop': 20})
        ]),
        html.Div(className='five columns', children=[
            dcc.Loading(dcc.Graph(id='timeseries-plot'))
        ]),
    ]),
    dcc.Store(id='selected-point-store', data={'lat': 52.52, 'lon': 13.40})  # Default to Berlin
])

# --- Callbacks ---

@app.callback(
    Output('variable-dropdown', 'options'),
    Output('variable-dropdown', 'value'),
    Input('cycle-dropdown', 'value'))
def update_variable_dropdown(cycle_value):
    ds = load_dataset(cycle_value)
    if ds is None:
        return [], None
    
    available_vars = [k for k in VARIABLE_CONFIG if k in ds.variables]
    options = [{'label': VARIABLE_CONFIG[k]['label'], 'value': k} for k in available_vars]
    
    default_var = available_vars[0] if available_vars else None
    return options, default_var

@app.callback(
    Output('time-slider', 'min'),
    Output('time-slider', 'max'),
    Output('time-slider', 'value'),
    Output('time-slider', 'marks'),
    Input('cycle-dropdown', 'value'))
def update_time_slider(cycle_value):
    ds = load_dataset(cycle_value)
    if ds is None:
        return 0, 0, 0, {}

    time_coords = pd.to_datetime(ds.time.values)
    max_time = len(time_coords) - 1
    marks = {i: dt.strftime('%m-%d %H:%M') for i, dt in enumerate(time_coords) if i % 4 == 0}
    
    return 0, max_time, 0, marks

@app.callback(
    Output('weather-map', 'figure'),
    Input('variable-dropdown', 'value'),
    Input('cycle-dropdown', 'value'),
    Input('time-slider', 'value'))
def update_map(selected_variable, cycle_value, time_index):
    ds = load_dataset(cycle_value)
    if ds is None or selected_variable is None or time_index is None:
        return go.Figure()

    # Ensure time_index is within bounds
    max_time = len(ds.time) - 1
    time_index = min(max(0, time_index), max_time)

    var_config = VARIABLE_CONFIG[selected_variable]
    data_slice = ds[selected_variable].isel(time=time_index)
    
    converted_data = var_config['convert'](data_slice)

    fig = go.Figure(go.Contour(
        z=converted_data.values,
        x=ds.longitude.values,
        y=ds.latitude.values,
        colorscale=var_config['colorscale'],
        colorbar_title=var_config['unit'],
        contours=dict(coloring='fill'),
        hoverinfo='x+y+z'
    ))

    fig.update_layout(
        title=f"{var_config['label']} at {pd.to_datetime(ds.time.values[time_index]).strftime('%Y-%m-%d %H:%M')} UTC",
        xaxis_title="Longitude",
        yaxis_title="Latitude",
        geo=dict(
            scope='europe',
            projection_type='mercator',
            center=dict(
                lon=(EUROPE_BOUNDS['lon_min'] + EUROPE_BOUNDS['lon_max']) / 2,
                lat=(EUROPE_BOUNDS['lat_min'] + EUROPE_BOUNDS['lat_max']) / 2
            ),
            lataxis_range=[EUROPE_BOUNDS['lat_min'], EUROPE_BOUNDS['lat_max']],
            lonaxis_range=[EUROPE_BOUNDS['lon_min'], EUROPE_BOUNDS['lon_max']]
        ),
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
        return {'lat': point['y'], 'lon': point['x']}
    return current_point

@app.callback(
    Output('timeseries-plot', 'figure'),
    Input('variable-dropdown', 'value'),
    Input('selected-point-store', 'data'),
    Input('cycle-dropdown', 'value'),
    Input('time-slider', 'value'))
def update_timeseries(selected_variable, selected_point, cycle_value, time_index):
    ds = load_dataset(cycle_value)
    if ds is None or selected_variable is None or selected_point is None or time_index is None:
        return go.Figure()

    lat = selected_point['lat']
    lon = selected_point['lon']

    # Check if latitude is a dimension for selection method
    if 'latitude' in ds.dims:
        # GFS-style data with 1D lat/lon dimensions
        point_data = ds[selected_variable].sel(latitude=lat, longitude=lon, method='nearest')
    else:
        # MET-style data with 2D lat/lon coordinates on x/y dimensions
        # Find the nearest grid point index
        dist_sq = (ds.latitude - lat)**2 + (ds.longitude - lon)**2
        min_dist_idx = dist_sq.values.argmin()
        y_idx, x_idx = np.unravel_index(min_dist_idx, ds.latitude.shape)
        point_data = ds[selected_variable].isel(y=y_idx, x=x_idx)
    
    var_config = VARIABLE_CONFIG[selected_variable]
    timeseries_data = var_config['convert'](point_data)
    
    time_values = pd.to_datetime(ds.time.values)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=time_values,
        y=timeseries_data.values,
        mode='lines+markers',
        name=var_config['label']
    ))
    
    # Add a vertical line for the selected time step
    # Ensure time_index is within bounds
    max_time = len(time_values) - 1
    time_index = min(max(0, time_index), max_time)
    selected_time = time_values[time_index]
    fig.add_vline(x=selected_time, line_width=2, line_dash="dash", line_color="red")
    
    fig.update_layout(
        title=f"Forecast for {var_config['label']}<br>Lat: {lat:.2f}, Lon: {lon:.2f}",
        xaxis_title="Time (UTC)",
        yaxis_title=f"{var_config['label']} ({var_config['unit']})",
        margin={"r":20,"t":80,"l":20,"b":20}
    )
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
