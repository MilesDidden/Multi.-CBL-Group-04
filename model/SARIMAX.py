#from DB_utils import DBhandler

# need to specify 
from model.DB_utils import DBhandler 

import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
import plotly.graph_objects as go


def timeseries(ward_code: str, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db"):
    # Load DBhandler to get data path
<<<<<<< HEAD
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")
    db_handler.close_connection_db()
=======
    db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)

    df = db_handler.query(
        f"""
        SELECT
            *
        FROM
            temp_crime_{ward_code}
        """
    )
>>>>>>> f5326619ce8027721403172662767da526ea8286

    db_handler.close_connection_db()

    df["month"] = pd.to_datetime(df['month'])

    # Filter and aggregate by month
    df = df.groupby("month").agg(
        num_of_crimes=("crime_id", "count"),
        avg_imd=("average_imd_decile", "mean")
    ).sort_index()

    df = df.asfreq("MS")  # Monthly frequency
    df["num_of_crimes"] = df["num_of_crimes"].fillna(0)
    df["avg_imd"] = df["avg_imd"].interpolate()

    # Check stationarity using ADFuller
    adfuller_test = adfuller(df["num_of_crimes"])
    # print(f"ADF p-value: {adfuller_test[1]}")

    if adfuller_test[1] < 0.05:
        # stationary
        p_d_q = (1, 1, 0)
        p_d_q_s = (1, 1, 0, 12)
    else:
        # Non-stationary, hence need to make stationary by using differencing
        p_d_q = (1, 1, 1)
        p_d_q_s = (1, 1, 1, 12)

    # Fit SARIMAX model
    sarimax = sm.tsa.statespace.SARIMAX(
        df["num_of_crimes"],
        exog=df["avg_imd"],
        order=p_d_q,
        seasonal_order=p_d_q_s,
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
            order=p_d_q,
            seasonal_order=p_d_q_s,
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
        order=p_d_q,
        seasonal_order=p_d_q_s,
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    step_model = model.filter(results.params)
    forecast_next_month = step_model.forecast(steps=1, exog=exog_forecast).iloc[0]

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
