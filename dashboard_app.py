import dash
from dash import dcc, html, Input, Output, State
from dash.exceptions import PreventUpdate
# import plotly.express as px
import pandas as pd
import io
from dash.dcc import Download
import plotly.graph_objects as go

from utils.data_loader import load_ward_options, check_accesiblity_db, prevent_imd_from_reaching_extreme_points
from model.SARIMAX import timeseries
from model.ML_utils import create_temp_table, delete_temp_table
from model.KMeans import run_kmeans_weighted, plot_kmeans_clusters


WARD_OPTIONS = load_ward_options()
DB_LOC = "../data/"
DB_NAME = "crime_data_UK_v4.db"

app = dash.Dash(__name__)
app.title = "Police Resource Dashboard"

app.layout = html.Div([
    html.H1("Police Resource Planning Dashboard", style={"textAlign": "center"}),

    html.Div("Forecast monthly burglary counts and simulate optimal police deployments across London wards.",
             style={"textAlign": "center", "color": "#003366", "marginTop": "10px", "marginBottom": "20px"}),

    html.Div([
        html.H3("Simulation Parameters", style={"textAlign": "center"}),

        html.Label("Select Ward:"),
        dcc.Dropdown(
            id="ward-dropdown",
            options=WARD_OPTIONS,
            value=None,
            placeholder="Select a ward"
        ),

        html.Br(),

        html.Label("Number of Officers:"),
        dcc.Slider(
            id="officer-slider",
            min=0,
            max=200,
            step=10,
            value=100,
            marks={i: str(i) for i in range(0, 201, 10)}
        ),

        html.Br(),

        html.Div(html.Button("Run Simulation", id="simulate-button", n_clicks=0),
                 style={"textAlign": "center"}),

        html.Br()
    ], style={
        "width": "70%", "margin": "auto", "padding": "20px",
        "border": "1px solid #ccc", "borderRadius": "10px"
    }),

    dcc.Loading(id="loading-output", type="circle", children=html.Div(id="results-section", children=[
        html.Div(id="forecast-text", style={"textAlign": "center", "fontSize": "20px", "fontWeight": "bold"}),
        html.Br(),

        dcc.Tabs(id="result-tabs", value="forecast-tab", children=[
            dcc.Tab(label="📈 Forecast Visualization", value="forecast-tab"),
            dcc.Tab(label="📍 Officer Deployment Map", value="deployment-tab")
        ]),
        html.Div(id="tab-content"),
        html.Br(),

        html.Div(id="download-section", children=[
            html.Button("Download Officer Locations as CSV", id="download-button"),
            Download(id="download-cluster-data")
        ], style={"textAlign": "center", "display": "none"})
    ], style={"display": "none"})),

    #Hidden Stores for Passing Figures and Data Between Callbacks
    dcc.Store(id="forecast-fig-store"),
    dcc.Store(id="deployment-fig-store"),
    dcc.Store(id="officer-data-store")
])

@app.callback(
    Output("forecast-fig-store", "data"),
    Output("forecast-text", "children"),
    Output("deployment-fig-store", "data"),
    Output("officer-data-store", "data"),
    Output("results-section", "style"),
    Output("download-section", "style"),
    Input("simulate-button", "n_clicks"),
    State("ward-dropdown", "value"),
    State("officer-slider", "value")
)
def run_simulation(n_clicks, ward_code, num_officers):
    if not n_clicks or ward_code is None:
        raise PreventUpdate

    try:
        db_loc, db_name = check_accesiblity_db(DB_LOC, DB_NAME)

        create_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

        fig_forecast, forecast_value, imd = timeseries(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

        # Prevent weights from collapsing to 0 (which breaks np.average)
        # IMD scale is from 1 (most deprived) to 10 (least)
        imd = prevent_imd_from_reaching_extreme_points(imd)

        #check both value and figure contents
        if not isinstance(forecast_value, (int, float)) or not fig_forecast or not fig_forecast.data:
            return {}, "❌ Forecast unavailable", {}, [], {"display": "block"}, {"display": "none"}

        forecast_text = f"📈 Forecast for Next Month: {forecast_value:.2f} Burglaries"

        officer_locations, crime_location_df = run_kmeans_weighted(
            ward_code=ward_code,
            n_crimes=forecast_value,
            imd_value=imd,
            n_clusters=num_officers,
            db_loc=db_loc,
            db_name=db_name
        )

        fig_map = plot_kmeans_clusters(
            clustered_data=crime_location_df, 
            centroids=officer_locations, 
            ward_code=ward_code, 
            db_loc=db_loc, 
            db_name=db_name
        )

        delete_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

        return (
           fig_forecast.to_dict(),  
           forecast_text,
           fig_map.to_dict(),       
           pd.DataFrame(officer_locations).to_dict("records"),
           {"display": "block"},
           {"display": "block"}
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}, "❌ An error occurred during simulation.", {}, [], {"display": "block"}, {"display": "none"}


@app.callback(
    Output("tab-content", "children"),
    Input("result-tabs", "value"),
    State("forecast-fig-store", "data"),
    State("deployment-fig-store", "data")
)

def render_tab(tab, forecast_fig, deployment_fig):
    if tab == "forecast-tab":
        try:
            if forecast_fig and isinstance(forecast_fig, dict) and "data" in forecast_fig and forecast_fig["data"]:
                return dcc.Graph(figure=go.Figure(forecast_fig))
        except Exception as e:
            print(f"⚠️ Failed to render forecast figure: {e}")
        return html.Div("⚠️ Forecast plot not available.")

    elif tab == "deployment-tab":
        try:
            if deployment_fig and isinstance(deployment_fig, dict) and "data" in deployment_fig and deployment_fig["data"]:
                return dcc.Graph(figure=go.Figure(deployment_fig))
        except Exception as e:
            print(f"⚠️ Failed to render deployment figure: {e}")
        return html.Div("⚠️ Deployment map not available.")

    return html.Div("Invalid tab selected.")

# Export CSV
@app.callback(
    Output("download-cluster-data", "data"),
    Input("download-button", "n_clicks"),
    State("officer-data-store", "data"),
    prevent_initial_call=True
)
def export_csv(n_clicks, data):
    if not data:
        raise PreventUpdate
    df = pd.DataFrame(data)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return dict(content=buffer.getvalue(), filename="officer_clusters.csv", type="text/csv")

if __name__ == "__main__":
    import threading
    import webbrowser

    port = 8050  # or any other port you'd like
    url = f"http://127.0.0.1:{port}/"

    # Start a browser tab shortly after the server starts
    threading.Timer(1.0, lambda: webbrowser.open_new(url)).start()

    app.run(debug=True, port=port, use_reloader=False)
