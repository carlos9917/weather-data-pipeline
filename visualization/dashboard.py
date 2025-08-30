"""
Dashboard Module
Interactive dashboard for visualizing GFS wind power density data
"""

import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import duckdb
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
import sys
sys.path.append('config')
from config import DATABASE_PATH, DASHBOARD_HOST, DASHBOARD_PORT, DASHBOARD_DEBUG

class GFSDashboard:
    def __init__(self):
        self.setup_logging()
        self.app = dash.Dash(__name__)
        self.setup_layout()
        self.setup_callbacks()
        
    def setup_logging(self):
        """Setup logging"""
        logger.info("GFS Dashboard initialized")
        
    def get_available_dates(self):
        """Get available forecast dates from database"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            query = "SELECT DISTINCT forecast_date FROM gfs_forecasts ORDER BY forecast_date DESC"
            dates = conn.execute(query).fetchdf()['forecast_date'].tolist()
            conn.close()
            return dates
        except Exception as e:
            logger.error(f"Failed to get available dates: {e}")
            return []
            
    def get_available_cycles(self, date):
        """Get available cycles for a specific date"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            query = "SELECT DISTINCT cycle FROM gfs_forecasts WHERE forecast_date = ? ORDER BY cycle"
            cycles = conn.execute(query, [date]).fetchdf()['cycle'].tolist()
            conn.close()
            return cycles
        except Exception as e:
            logger.error(f"Failed to get available cycles: {e}")
            return []
            
    def load_forecast_data(self, date, cycle):
        """Load forecast data for specific date and cycle"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            query = """
                SELECT * FROM gfs_forecasts 
                WHERE forecast_date = ? AND cycle = ?
                ORDER BY forecast_hour, lat, lon
            """
            df = conn.execute(query, [date, cycle]).fetchdf()
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to load forecast data: {e}")
            return pd.DataFrame()
            
    def load_country_rankings(self, date, cycle):
        """Load country rankings for specific date and cycle"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            query = """
                SELECT * FROM country_rankings 
                WHERE forecast_date = ? AND cycle = ?
                ORDER BY rank
            """
            df = conn.execute(query, [date, cycle]).fetchdf()
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to load country rankings: {e}")
            return pd.DataFrame()
            
    def load_plant_forecast_data(self, date, cycle):
        """Load wind power plant forecast data for specific date and cycle"""
        try:
            conn = duckdb.connect(DATABASE_PATH)
            query = """
                SELECT * FROM wind_power_plant_forecasts 
                WHERE forecast_date = ? AND cycle = ?
                ORDER BY forecast_hour, lat, lon
            """
            df = conn.execute(query, [date, cycle]).fetchdf()
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Failed to load plant forecast data: {e}")
            return pd.DataFrame()

    def create_wind_power_map(self, df, forecast_hour):
        """Create wind power density map for specific forecast hour"""
        if df.empty:
            return go.Figure()
            
        df_hour = df[df['forecast_hour'] == forecast_hour]
        
        if df_hour.empty:
            return go.Figure()
            
        fig = px.scatter_mapbox(
            df_hour,
            lat='lat',
            lon='lon',
            color='wind_power_density',
            size='wind_power_density',
            hover_data=['temp_2m', 'u_wind_100m', 'v_wind_100m'],
            color_continuous_scale='Viridis',
            size_max=15,
            zoom=3,
            center={'lat': 52.5, 'lon': 13.4},
            title=f'Wind Power Density - Forecast Hour {forecast_hour}'
        )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            height=600,
            margin={"r":0,"t":50,"l":0,"b":0}
        )
        
        return fig
        
    def create_daily_average_maps(self, df):
        """Create daily average wind power density maps"""
        if df.empty:
            return go.Figure()
            
        # Group by day (every 8 forecast hours = 1 day)
        df['day'] = df['forecast_hour'] // 24
        daily_avg = df.groupby(['day', 'lat', 'lon'])['wind_power_density'].mean().reset_index()
        
        # Create subplots for each day
        days = sorted(daily_avg['day'].unique())
        n_days = len(days)
        
        if n_days == 0:
            return go.Figure()
            
        # Create subplot titles
        subplot_titles = [f'Day {day+1}' for day in days]
        
        fig = make_subplots(
            rows=1, cols=n_days,
            subplot_titles=subplot_titles,
            specs=[[{'type': 'scattermapbox'}] * n_days]
        )
        
        for i, day in enumerate(days):
            day_data = daily_avg[daily_avg['day'] == day]
            
            fig.add_trace(
                go.Scattermapbox(
                    lat=day_data['lat'],
                    lon=day_data['lon'],
                    mode='markers',
                    marker=dict(
                        size=8,
                        color=day_data['wind_power_density'],
                        colorscale='Viridis',
                        showscale=(i == 0)  # Show colorbar only for first subplot
                    ),
                    text=day_data['wind_power_density'].round(2),
                    hovertemplate='<b>Wind Power Density</b><br>%{text} W/mÂ²<extra></extra>'
                ),
                row=1, col=i+1
            )
            
        fig.update_layout(
            height=400,
            showlegend=False,
            title_text="Daily Average Wind Power Density Maps"
        )
        
        # Update mapbox settings for all subplots
        for i in range(n_days):
            fig.update_layout({
                f'mapbox{i+1}': dict(
                    style="open-street-map",
                    center={'lat': 52.5, 'lon': 13.4},
                    zoom=2
                )
            })
            
        return fig
        
    def create_country_ranking_chart(self, df):
        """Create country ranking bar chart"""
        if df.empty:
            return go.Figure()
            
        fig = px.bar(
            df,
            x='avg_wind_power_density',
            y='country',
            orientation='h',
            title='Country Rankings by Average Wind Power Density',
            labels={
                'avg_wind_power_density': 'Average Wind Power Density (W/mÂ²)',
                'country': 'Country'
            }
        )
        
        fig.update_layout(
            height=400,
            yaxis={'categoryorder': 'total ascending'}
        )
        
        return fig
        
    def create_time_series_chart(self, df):
        """Create time series chart of wind power density"""
        if df.empty:
            return go.Figure()
            
        # Calculate average wind power density by forecast hour
        hourly_avg = df.groupby('forecast_hour')['wind_power_density'].mean().reset_index()
        
        fig = px.line(
            hourly_avg,
            x='forecast_hour',
            y='wind_power_density',
            title='Average Wind Power Density Over Forecast Period',
            labels={
                'forecast_hour': 'Forecast Hour',
                'wind_power_density': 'Average Wind Power Density (W/mÂ²)'
            }
        )
        
        fig.update_layout(height=400)
        
        return fig
        
    def setup_layout(self):
        """Setup dashboard layout"""
        available_dates = self.get_available_dates()
        
        self.app.layout = html.Div([
            html.H1("GFS Wind Power Density Dashboard", 
                   style={'textAlign': 'center', 'marginBottom': 30}),
            
            # Control panel
            html.Div([
                html.Div([
                    html.Label("Select Date:"),
                    dcc.Dropdown(
                        id='date-dropdown',
                        options=[{'label': date, 'value': date} for date in available_dates],
                        value=available_dates[0] if available_dates else None,
                        style={'width': '200px'}
                    )
                ], style={'display': 'inline-block', 'marginRight': 20}),
                
                html.Div([
                    html.Label("Select Cycle:"),
                    dcc.Dropdown(
                        id='cycle-dropdown',
                        style={'width': '100px'}
                    )
                ], style={'display': 'inline-block', 'marginRight': 20}),
                
                html.Div([
                    html.Label("Forecast Hour:"),
                    dcc.Slider(
                        id='hour-slider',
                        min=0,
                        max=72,
                        step=3,
                        value=0,
                        marks={i: str(i) for i in range(0, 73, 12)},
                        tooltip={"placement": "bottom", "always_visible": True}
                    )
                ], style={'display': 'inline-block', 'width': '300px'})
                
            ], style={'marginBottom': 30, 'padding': 20, 'backgroundColor': '#f0f0f0'}),
            
            dcc.Tabs(id="tabs", value='tab-general', children=[
                dcc.Tab(label='General Forecast', value='tab-general'),
                dcc.Tab(label='Wind Power Plants', value='tab-plants'),
            ]),
            html.Div(id='tabs-content')
        ])
        
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        
        @self.app.callback(
            Output('cycle-dropdown', 'options'),
            Output('cycle-dropdown', 'value'),
            Input('date-dropdown', 'value')
        )
        def update_cycle_dropdown(selected_date):
            if not selected_date:
                return [], None
                
            cycles = self.get_available_cycles(selected_date)
            options = [{'label': cycle, 'value': cycle} for cycle in cycles]
            value = cycles[0] if cycles else None
            
            return options, value
            
        @self.app.callback(
            Output('tabs-content', 'children'),
            Input('tabs', 'value')
        )
        def render_content(tab):
            if tab == 'tab-general':
                return html.Div([
                    dcc.Graph(id='wind-power-map'),
                    dcc.Graph(id='daily-average-maps'),
                    html.Div([
                        html.Div([
                            dcc.Graph(id='country-rankings')
                        ], style={'width': '50%', 'display': 'inline-block'}),
                        
                        html.Div([
                            dcc.Graph(id='time-series')
                        ], style={'width': '50%', 'display': 'inline-block'})
                    ])
                ])
            elif tab == 'tab-plants':
                return html.Div([
                    dcc.Graph(id='plant-map'),
                    dcc.Graph(id='plant-time-series')
                ])

        @self.app.callback(
            Output('wind-power-map', 'figure'),
            Output('daily-average-maps', 'figure'),
            Output('country-rankings', 'figure'),
            Output('time-series', 'figure'),
            Input('date-dropdown', 'value'),
            Input('cycle-dropdown', 'value'),
            Input('hour-slider', 'value'),
            prevent_initial_call=True
        )
        def update_general_charts(selected_date, selected_cycle, selected_hour):
            if not selected_date or not selected_cycle:
                empty_fig = go.Figure()
                return empty_fig, empty_fig, empty_fig, empty_fig
                
            # Load data
            forecast_data = self.load_forecast_data(selected_date, selected_cycle)
            country_data = self.load_country_rankings(selected_date, selected_cycle)
            
            # Create charts
            wind_map = self.create_wind_power_map(forecast_data, selected_hour)
            daily_maps = self.create_daily_average_maps(forecast_data)
            country_chart = self.create_country_ranking_chart(country_data)
            time_series = self.create_time_series_chart(forecast_data)
            
            return wind_map, daily_maps, country_chart, time_series

        @self.app.callback(
            Output('plant-map', 'figure'),
            Output('plant-time-series', 'figure'),
            Input('date-dropdown', 'value'),
            Input('cycle-dropdown', 'value'),
            prevent_initial_call=True
        )
        def update_plant_charts(selected_date, selected_cycle):
            if not selected_date or not selected_cycle:
                return go.Figure(), go.Figure()

            plant_data = self.load_plant_forecast_data(selected_date, selected_cycle)

            if plant_data.empty:
                return go.Figure(), go.Figure()

            # Plant Map
            plant_map = px.scatter_mapbox(
                plant_data[plant_data['forecast_hour'] == 0],
                lat='lat',
                lon='lon',
                hover_name='lat',
                zoom=4,
                center={'lat': 52.5, 'lon': 13.4},
                title='Wind Power Plant Locations'
            )
            plant_map.update_layout(mapbox_style="open-street-map")

            # Plant Time Series
            plant_time_series = px.line(
                plant_data,
                x='forecast_hour',
                y='wind_power_density',
                color='lat',
                title='Wind Power Density Forecast for Power Plants'
            )

            return plant_map, plant_time_series
            
    def run_server(self):
        """Run the dashboard server"""
        logger.info(f"Starting dashboard server on {DASHBOARD_HOST}:{DASHBOARD_PORT}")
        self.app.run_server(
            host=DASHBOARD_HOST,
            port=DASHBOARD_PORT,
            debug=DASHBOARD_DEBUG
        )

if __name__ == "__main__":
    dashboard = GFSDashboard()
    dashboard.run_server()
