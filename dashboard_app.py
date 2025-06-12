import dash
from dash import dcc, html, Input, Output, State, no_update
from dash.exceptions import PreventUpdate
import pandas as pd
import io
from dash.dcc import Download
import plotly.graph_objects as go
import threading
import webbrowser
import osmnx as ox
from queue import Queue


from utils.data_loader import (
    load_ward_options, 
    check_accesiblity_db, 
    prevent_imd_from_reaching_extreme_points
    )
from model.SARIMAX import (
    timeseries
    )
from model.ML_utils import (
    create_temp_table, 
    delete_temp_table
    )
from model.KMeans import (
    run_kmeans_weighted, 
    plot_kmeans_clusters, 
    calc_avg_distance_between_crime_and_officer, 
    calc_street_distance_between_crime_and_officer
    )

WARD_OPTIONS = load_ward_options()
DB_LOC = "../data/"
DB_NAME = "crime_data_UK_v4.db"

LONDON_GRAPHML = ox.load_graphml("data/london_map_drive.graphml")

strat_report_queue = Queue()
street_distance_queue = Queue()

app = dash.Dash(__name__)
app.title = "Police Resource Dashboard"

app.layout = html.Div([
    html.H1("Police Resource Planning Dashboard", style={"textAlign": "center"}),

    html.Div("Forecast monthly burglary counts and simulate optimal police deployments across London wards.",
             style={"textAlign": "center", "color": "#003366", "marginTop": "10px", "marginBottom": "20px"}),

    html.Div([
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
            "flex": "0 0 70%",  # left column fixed to 70%
            "padding": "20px",
            "border": "1px solid #ccc",
            "borderRadius": "10px",
            "marginRight": "10px"  # space between columns
        }),

        html.Div([
            html.H3("Current Strategy Parameters", style={"textAlign": "center"}),
            html.Div(id="police-officer-number", style={"fontSize": "16px", "marginTop": "10px"}),
            html.Div(id="number-of-predicted-crimes", style={"fontSize": "16px", "marginTop": "10px"}),
            html.Div(id="mean-absolute-error-timeseries", style={"fontSize": "16px", "marginTop": "10px"}),
            html.Div(id="mean-euclidean-distance-text", style={"fontSize": "16px", "marginTop": "10px"}),
            html.Div(id="max-euclidean-distance-text", style={"fontSize": "16px", "marginTop": "10px"}),
            html.Div(id="mean-street-distance-text", style={"fontSize": "16px", "marginTop": "10px"}),
            html.Div(id="max-street-distance-text", style={"fontSize": "16px", "marginTop": "10px"}),
        ], style={
            "flex": "1",  # right column takes remaining 30%
            "padding": "20px",
            "border": "1px solid #ccc",
            "borderRadius": "10px"
        })
    ], style={
        "display": "flex",         # <--- this is the key!
        "flexDirection": "row",
        "width": "90%",            # optional: total width
        "margin": "auto",
        "padding": "20px",
        "border": "1px solid #ccc",
        "borderRadius": "10px"
    }),

    dcc.Loading(id="loading-output", type="circle", children=html.Div(id="results-section", children=[
        
        html.Br(),
        dcc.Tabs(id="result-tabs", value="forecast-tab", children=[
            dcc.Tab(label="📈 Forecast Visualization", value="forecast-tab"),
            dcc.Tab(label="📍 Officer Deployment Map", value="deployment-tab")
        ]),
        dcc.Graph(id="forecast-graph", style={"display": "block"}),
        dcc.Graph(id="deployment-graph", style={"display": "none"}),
        html.Br(),

        html.Div(id="download-section", children=[
            html.Button("Download Officer Locations as CSV", id="download-button"),
            Download(id="download-cluster-data")
        ], style={"textAlign": "center", "display": "none"})
    ], style={"display": "none"})),

    #Hidden Stores for Passing Figures and Data Between Callbacks
    dcc.Store(id="forecast-fig-store"),
    dcc.Store(id="deployment-fig-store"),
    dcc.Store(id="officer-data-store"),
    dcc.Store(id="street-distance-store"),
    dcc.Store(id="strategy-report-store"),

    # Add interval to trigger queues
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
    dcc.Interval(id="street-interval", interval=1000, n_intervals=0)
])

@app.callback(
    Output("forecast-fig-store", "data"),
    Output("deployment-fig-store", "data"),
    Output("officer-data-store", "data"),
    Output("results-section", "style"),
    Output("download-section", "style"),
    Input("simulate-button", "n_clicks"),
    State("ward-dropdown", "value"),
    State("officer-slider", "value")
)
def run_simulation(n_clicks, ward_code, num_officers):

    street_distance_queue.put({})
    strat_report_queue.put({})

    if not n_clicks or ward_code is None:
        raise PreventUpdate

    try:
        db_loc, db_name = check_accesiblity_db(DB_LOC, DB_NAME)

        create_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

        fig_forecast, forecast_value, imd, mae = timeseries(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

        # Prevent weights from collapsing to 0 (which breaks np.average)
        # IMD scale is from 1 (most deprived) to 10 (least)
        imd = prevent_imd_from_reaching_extreme_points(imd)

        #check both value and figure contents
        if not isinstance(forecast_value, (int, float)) or not fig_forecast or not fig_forecast.data:
            return {}, {}, [], {"display": "block"}, {"display": "none"}

        officer_locations, crime_location_df = run_kmeans_weighted(
            ward_code=ward_code,
            n_crimes=forecast_value,
            imd_value=imd,
            n_clusters=num_officers,
            db_loc=db_loc,
            db_name=db_name
        )

        ward_name_df = pd.DataFrame(WARD_OPTIONS)

        fig_map = plot_kmeans_clusters(
            clustered_data=crime_location_df, 
            centroids=officer_locations, 
            ward_code=ward_code, 
            ward_name=ward_name_df[ward_name_df["value"] == ward_code]["label"].values[0],
            db_loc=db_loc, 
            db_name=db_name
        )

        # Euclidean distance
        mean_euclidean_distance, max_euclidean_distance = calc_avg_distance_between_crime_and_officer(
            clustered_data=crime_location_df, centroids=officer_locations)
        
        # Street distance
        def background_street_distance(clustered_data, centroids, graph):

            try:
                print("🔄 Starting background street distance calculation...")
                mean_street_distance, max_street_distance = calc_street_distance_between_crime_and_officer(
                    clustered_data=clustered_data, centroids=centroids, graph=graph
                )
                print(f"✅ Done: mean={mean_street_distance}, max={max_street_distance}")

                street_distance_queue.put({
                    "mean": mean_street_distance,
                    "max": max_street_distance
                })

                delete_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

            except Exception as e:
                print(f"⚠️ Error in background street distance calculation: {e}")

        threading.Thread(
            target=background_street_distance,
            args=(crime_location_df, officer_locations, LONDON_GRAPHML),
            daemon=True
        ).start()

        if forecast_value < num_officers:
            assigned_officer_text = (f"Number of assigned officers: {num_officers} - "+ 
                                     "Recommendation: Do not use more officers than forecasted crimes!")
        else:
            assigned_officer_text = f"Number of assigned officers: {num_officers}"
        forecast_text = f"📈 Forecast for next month: {int(round(forecast_value, 0))} burglaries"
        mae_forecast_text = f"Average error of forecasts: {round(mae, 2)} burglaries"
        mean_euclidean_distance_text = f"Average euclidean distance: {round(mean_euclidean_distance/1000, 3)} km"
        max_euclidean_distance_text = f"Maximum euclidean distance: {round(max_euclidean_distance/1000, 3)} km"

        strat_report_queue.put({
            "num_police_officers":assigned_officer_text,
            "num_crimes":forecast_text,
            "mae_forecast":mae_forecast_text,
            "mean_distance":mean_euclidean_distance_text,
            "max_distance":max_euclidean_distance_text
        })

        return (
           fig_forecast.to_dict(),
           fig_map.to_dict(),
           pd.DataFrame(officer_locations).to_dict("records"),
           {"display": "block"},
           {"display": "block"}
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}, {}, [], {"display": "block"}, {"display": "none"}


@app.callback(
    Output("forecast-graph", "figure"),
    Input("forecast-fig-store", "data")
)
def update_forecast_figure(forecast_fig):
    if forecast_fig and isinstance(forecast_fig, dict) and "data" in forecast_fig and forecast_fig["data"]:
        return go.Figure(forecast_fig)
    return go.Figure()  # empty fallback


@app.callback(
    Output("deployment-graph", "figure"),
    Input("deployment-fig-store", "data")
)
def update_deployment_figure(deployment_fig):
    if deployment_fig and isinstance(deployment_fig, dict) and "data" in deployment_fig and deployment_fig["data"]:
        return go.Figure(deployment_fig)
    return go.Figure()


@app.callback(
    Output("forecast-graph", "style"),
    Output("deployment-graph", "style"),
    Input("result-tabs", "value")
)
def switch_tabs(tab):
    if tab == "forecast-tab":
        return {"display": "block"}, {"display": "none"}
    elif tab == "deployment-tab":
        return {"display": "none"}, {"display": "block"}
    return {"display": "none"}, {"display": "none"}


@app.callback(
        Output("strategy-report-store", "data"),
        Input("interval", "n_intervals")
)
def update_strategy_parameters_store(n):

    if not strat_report_queue.empty():
        data = strat_report_queue.get()
        return data
    else:
        raise PreventUpdate


@app.callback(
        Output("police-officer-number", "children"),
        Output("number-of-predicted-crimes", "children"),
        Output("mean-absolute-error-timeseries", "children"),
        Output("mean-euclidean-distance-text", "children"),
        Output("max-euclidean-distance-text", "children"),
        Input("simulate-button", "n_clicks"),
        Input("strategy-report-store", "data"),
        prevent_initial_call=True
)
def display_strategy_parameters(n_clicks, data):
    if n_clicks is None or n_clicks == 0:
        raise PreventUpdate
    
    # If no data or empty dict → show calculating message
    if data is None or data == {}:
        return (
            " ", " ",
            "Calculating parameters ...",
            " ", " ",
        )
    
    try:
        return (
            data["num_police_officers"],
            data["num_crimes"],
            data["mae_forecast"],
            data["mean_distance"],
            data["max_distance"]
        )
    except Exception:
        return (
            " ", " ",
            "⚠️ Error showing parameters", 
            " ", " "
        )


@app.callback(
    Output("street-distance-store", "data"),
    Input("simulate-button", "n_clicks"),
    Input("street-interval", "n_intervals"),
    prevent_initial_call=True
)
def update_or_reset_street_distance(n_clicks, n_intervals):
    ctx = dash.callback_context

    if not ctx.triggered:
        return no_update

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "simulate-button":
        print("Resetting street-distance-store to {}")
        return {}

    elif trigger_id == "street-interval":
        if not street_distance_queue.empty():
            data = street_distance_queue.get()
            print(f"Updating street-distance-store with result: {data}")
            return data
        else:
            # Instead of PreventUpdate → return no_update → lets downstream callbacks run.
            return no_update


@app.callback(
    Output("mean-street-distance-text", "children"),
    Output("max-street-distance-text", "children"),
    Input("simulate-button", "n_clicks"),
    Input("street-distance-store", "data"),
    prevent_initial_call=True
)
def display_street_distance(n_clicks, data):
    # If no click → show nothing
    if n_clicks is None or n_clicks == 0:
        raise PreventUpdate

    # If no data or empty dict → show calculating message
    if data is None or data == {}:
        return "Calculating street distance...", " "

    # If data is valid → show distances
    try:
        return f"Average street distance: {round(data['mean']/1000, 3)} km", f"Maximum street distance: {round(data['max']/1000, 3)} km"
    except Exception:
        return "⚠️ Error displaying street distance.", " "


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

    port = 8050  # or any other port you'd like
    url = f"http://127.0.0.1:{port}/"

    # Start a browser tab shortly after the server starts
    threading.Timer(1.0, lambda: webbrowser.open_new(url)).start()

    app.run(debug=True, port=port, use_reloader=False)
