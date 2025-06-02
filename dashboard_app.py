import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
from model.SARIMAX import timeseries  
import plotly.express as px
import pandas as pd
from utils.data_loader import load_crime_data  
from utils.data_loader import load_ward_options


WARD_OPTIONS = load_ward_options()

app.layout = html.Div([
    html.H1("Burglary Forecasting & Officer Deployment", style={"textAlign": "center"}),

    html.Div([
        html.Label("Select Ward:"),
        dcc.Dropdown(
            id="ward-dropdown",
            options=WARD_OPTIONS,
            value="E05000138"
        ),

        html.Label("Number of Officers:"),
        dcc.Slider(
            id="officer-slider",
            min=0,
            max=200,
            step=1,
            value=100,
            marks={0: "0", 100: "100", 200: "200"}
        ),

        html.Br(),
        html.Button("Run Simulation", id="simulate-button", n_clicks=0),
        html.Br(), html.Br(),

        dcc.Graph(id="forecast-graph"),
        html.Br(),
        dcc.Graph(id="deployment-map")
    ], style={"width": "70%", "margin": "auto"})
])

# Forecast callback
@app.callback(
    Output("forecast-graph", "figure"),
    Input("simulate-button", "n_clicks"),
    State("ward-dropdown", "value"),
    State("officer-slider", "value")
)
def update_forecast_graph(n_clicks, ward_code, num_officers):
    if n_clicks == 0:
        return {}
    
    fig, forecast_value = timeseries(ward_code)
    return fig

# Deployment map callback (placeholder map)
@app.callback(
    Output("deployment-map", "figure"),
    Input("simulate-button", "n_clicks"),
    State("ward-dropdown", "value"),
    State("officer-slider", "value")
)
def update_deployment_map(n_clicks, ward_code, num_officers):
    if n_clicks == 0:
        return {}

    # Placeholder random coordinates in London
    officer_positions = pd.DataFrame({
        "lat": [51.51, 51.52, 51.50],
        "lon": [-0.12, -0.10, -0.11],
        "officer_id": ["Officer A", "Officer B", "Officer C"]
    })

    fig = px.scatter_mapbox(
        officer_positions,
        lat="lat",
        lon="lon",
        hover_name="officer_id",
        zoom=12,
        center={"lat": 51.51, "lon": -0.11}
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        title=f"Police Officer Deployment – Ward {ward_code}",
        margin={"r": 0, "t": 30, "l": 0, "b": 0}
    )
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)
