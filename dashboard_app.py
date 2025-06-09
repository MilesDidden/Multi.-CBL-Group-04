import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State  
import plotly.express as px
import pandas as pd
from utils.data_loader import load_crime_data  
from utils.data_loader import load_ward_options
from utils.data_loader import get_forecast_plot_and_value, get_clustered_map


WARD_OPTIONS = load_ward_options()

app = dash.Dash(__name__)
app.title = "Police Resource Dashboard"

app.layout = html.Div([
    html.H1("Burglary Forecasting & Officer Deployment", style={"textAlign": "center"}),
    
    html.Div("Select a ward and run the simulation.", 
             style={"textAlign": "center", "color": "#003366", "marginTop": "10px", "marginBottom": "20px"}),

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
        html.Div(id="forecast-text", style={"textAlign": "center", "marginTop": "20px"}),

        html.Br(),
        dcc.Graph(id="deployment-map")
    ], style={"width": "70%", "margin": "auto"})
])


@app.callback(
    Output("forecast-graph", "figure"),
    Output("forecast-text", "children"),
    Output("deployment-map", "figure"),
    Input("simulate-button", "n_clicks"),
    State("ward-dropdown", "value"),
    State("officer-slider", "value")
)
def update_dashboard(n_clicks, ward_code, num_officers):
    if n_clicks == 0 or ward_code is None:
        return {}, "", px.scatter_mapbox(pd.DataFrame(), lat=[], lon=[], title="")

    try:
        # Forecast
        fig_forecast, forecast_value = get_forecast_plot_and_value(ward_code)
        forecast_text = f"Predicted burglaries for next month: {forecast_value:.2f}" if isinstance(forecast_value, (int, float)) else forecast_value

        # Map
        fig_map = get_clustered_map(ward_code=ward_code, num_officers=num_officers, external_forecast_value=forecast_value)

        return fig_forecast, forecast_text, fig_map

    except Exception as e:
        print(f"Error: {e}")
        return {}, "An error occurred.", px.scatter_mapbox(pd.DataFrame(), lat=[], lon=[], title="Error")

if __name__ == "__main__":
    app.run_server(debug=True)
