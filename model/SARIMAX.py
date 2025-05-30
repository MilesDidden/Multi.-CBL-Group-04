from DB_utils import DBhandler

import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
import os
import plotly.graph_objects as go
import plotly.io as pio
from sklearn.cluster import KMeans


ward_code = "E05000138"
n_officers = 100


def timeseries(ward_code: str):
    # Load DBhandler to get data path
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")

    #ADDED TO K-MEANS class
    # # Create a temporary table for the selected ward
    # create_temp_table_query = f"""
    #     CREATE TABLE IF NOT EXISTS temp_crime_{ward_code} AS
    #     SELECT * FROM crime
    #     WHERE ward_code = '{ward_code}';
    # """
    # db_handler.update(create_temp_table_query) 

    # # Output sample of the temporary table to confirm correctness
    # sample_temp_table_query = f"SELECT * FROM temp_crime_{ward_code} LIMIT 5;"
    # temp_table_sample = db_handler.query(sample_temp_table_query)
    # print(f"Sample data from temp_crime_{ward_code}:\n", temp_table_sample)

    db_handler.close_connection_db()

    # Load CSV
    try:
        df = pd.read_csv(os.path.join(db_handler.db_loc, "temp_results.csv"), index_col=False, low_memory=False)
    except:
        raise ValueError("\nData file not found!\n")

    df["month"] = pd.to_datetime(df['month'])

    # Filter and aggregate by ward and month
    df = df[df["ward_code"] == ward_code]
    df = df.groupby("month").agg(
        num_of_crimes=("crime_id", "count"),
        avg_imd=("average_imd_decile", "mean")
    ).sort_index()

    df = df.asfreq("MS")  # Monthly frequency
    df["num_of_crimes"] = df["num_of_crimes"].fillna(0)
    df["avg_imd"] = df["avg_imd"].interpolate()

    # Check stationarity using ADFuller
    adfuller_test = adfuller(df["num_of_crimes"])
    print(f"ADF p-value: {adfuller_test[1]}")

    # Fit SARIMAX model
    sarimax = sm.tsa.statespace.SARIMAX(
        df["num_of_crimes"],
        exog=df["avg_imd"],
        order=(1, 1, 0),
        seasonal_order=(1, 0, 0, 12),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    results = sarimax.fit(disp=False)

    # Add forecast column
    df["forecast"] = pd.NA

    # One-step-ahead forecasts
    for t in range(10, len(df) - 1):
        endog_train = df["num_of_crimes"][:t + 1]
        exog_train = df["avg_imd"][:t + 1]
        exog_forecast = df["avg_imd"][t + 1:t + 2]

        model = sm.tsa.statespace.SARIMAX(
            endog_train,
            exog=exog_train,
            order=(1, 1, 0),
            seasonal_order=(1, 0, 0, 12),
            enforce_stationarity=False,
            enforce_invertibility=False
        )
        step_model = model.filter(results.params)
        forecast = step_model.forecast(steps=1, exog=exog_forecast)

        df.loc[df.index[t + 1], "forecast"] = forecast.values[0]

    # Forecast for the next unseen month
    last_index = df.index[-1]
    next_index = last_index + pd.DateOffset(months=1)

    endog_train = df["num_of_crimes"]
    exog_train = df["avg_imd"]
    exog_forecast = [df["avg_imd"].iloc[-1]]  # Use last value as estimate

    model = sm.tsa.statespace.SARIMAX(
        endog_train,
        exog=exog_train,
        order=(1, 1, 0),
        seasonal_order=(1, 0, 0, 12),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    step_model = model.filter(results.params)
    forecast_next_month = step_model.forecast(steps=1, exog=exog_forecast)[0]

    # Plotly figure
    fig = go.Figure()

    actual_x, actual_y = [], []
    forecast_x, forecast_y = [], []

    for i in range(10, len(df) - 1):
        t = df.index[i]
        t_next = df.index[i + 1]

        actual_now = df["num_of_crimes"].iloc[i]
        actual_next = df["num_of_crimes"].iloc[i + 1]
        forecast_next = df["forecast"].iloc[i + 1]

        if pd.isna(forecast_next):
            continue

        # Actual segment
        actual_x.extend([t, t_next, None])
        actual_y.extend([actual_now, actual_next, None])

        # Forecast segment
        forecast_x.extend([t, t_next, None])
        forecast_y.extend([actual_now, forecast_next, None])

    # Add single actual line
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=actual_x,
        y=actual_y,
        mode='lines+markers',
        line=dict(color='blue', width=2),
        name='Actual'
    ))

    # Add single forecast line
    fig.add_trace(go.Scatter(
        x=forecast_x,
        y=forecast_y,
        mode='lines',
        line=dict(color='orange', width=2),
        name='Forecast'
    ))

    # Final red forecast line to next unseen month
    fig.add_trace(go.Scatter(
        x=[last_index, next_index],
        y=[df["num_of_crimes"].iloc[-1], forecast_next_month],
        mode='lines',
        line=dict(color='red', width=2),
        name='Next Forecast'
    ))

    # Layout
    fig.update_layout(
        title="Forked Step-Ahead Forecast Visualization",
        xaxis_title="Month",
        yaxis_title="Number of Crimes",
        legend=dict(x=0.01, y=0.99),
        margin=dict(l=40, r=20, t=50, b=40)
    )

    return fig, forecast_next_month


if __name__ == "__main__":
    fig, forecasted_num_of_crimes = timeseries(ward_code=ward_code)
    pio.write_html(fig, file="forecast_plot.html", auto_open=True)
    # print(forecasted_num_of_crimes)


#ADDED TO K-MEANS class
# def run_kmeans(ward_code: str, n_clusters: int = 100):
#     # Initialize DB connection
#     db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")

#     # Query lat, long from temp table
#     crime_query = f"""
#     SELECT lat AS latitude, long AS longitude
#     FROM temp_crime_{ward_code}
#     WHERE lat IS NOT NULL AND long IS NOT NULL;
#     """
#     crime_locations = db_handler.query(crime_query)

#     if crime_locations.empty:
#         raise ValueError(f"No valid lat/long entries found for ward {ward_code}")

#     # Convert to NumPy array for clustering
#     coords = crime_locations[["latitude", "longitude"]].to_numpy()

#     # KMeans clustering
#     kmeans = KMeans(n_clusters=n_clusters, random_state=42)
#     kmeans.fit(coords)
#     centroids = kmeans.cluster_centers_

#     # Clean up: Drop temp table
#     drop_query = f"DROP TABLE IF EXISTS temp_crime_{ward_code};"
#     db_handler.update(drop_query)

#     db_handler.close_connection_db()

#     # Return centroids and full dataframe (optional: can be used for plotting)
#     crime_locations["cluster"] = kmeans.labels_
#     return centroids, crime_locations

# if __name__ == "__main__":
#     centroids, clustered_data = run_kmeans(ward_code)
#     print(f"Centroids for {ward_code}:\n", centroids)
