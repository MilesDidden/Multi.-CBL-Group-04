from DB_utils import DBhandler
import pandas as pd
import statsmodels.api as sm
import os
import matplotlib.pyplot as plt


if __name__ == "__main__":

    # Load DBhandler to get data path
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v3.db")
    db_handler.close_connection_db()

    # Load CSV
    df = pd.read_csv(os.path.join(db_handler.db_loc, "temp_results.csv"), index_col=False, low_memory=False)
    df["month"] = pd.to_datetime(df['month'])

    # Filter and aggregate by ward and month
    df = df[df["ward_code"] == "E05000138"]
    df = df.groupby("month").agg(
        num_of_crimes=("crime_id", "count"),
        avg_imd=("average_imd_decile", "mean")
    ).sort_index()

    # Set datetime index and frequency
    df = df.asfreq("MS")  # MS = Month Start

    # Handle missing values
    df["num_of_crimes"] = df["num_of_crimes"].fillna(0)
    df["avg_imd"] = df["avg_imd"].interpolate()

    # Fit SARIMAX model
    sarimax = sm.tsa.statespace.SARIMAX(
        df["num_of_crimes"],
        exog=df["avg_imd"],
        order=(1, 0, 0),
        seasonal_order=(1, 0, 0, 12),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    results = sarimax.fit(disp=False)

    # Prepare forecast column with NaNs
    df["forecast"] = pd.NA

    # One-step-ahead forecast loop
    for t in range(10, len(df) - 1):
        endog_train = df["num_of_crimes"][:t+1]
        exog_train = df["avg_imd"][:t+1]
        exog_forecast = df["avg_imd"][t+1:t+2]

        model = sm.tsa.statespace.SARIMAX(
            endog_train,
            exog=exog_train,
            order=(1, 0, 0),
            seasonal_order=(1, 0, 0, 12),
            enforce_stationarity=False,
            enforce_invertibility=False
        )
        step_model = model.filter(results.params)
        forecast = step_model.forecast(steps=1, exog=exog_forecast)

        df.loc[df.index[t+1], "forecast"] = forecast.values[0]

    # Plot forked visualization
    fig, ax = plt.subplots(figsize=(12, 5))

    for i in range(10, len(df) - 1):
        t = df.index[i]
        t_next = df.index[i + 1]

        actual_now = df["num_of_crimes"].iloc[i]
        actual_next = df["num_of_crimes"].iloc[i + 1]
        forecast_next = df["forecast"].iloc[i + 1]

        # Skip if forecast is missing
        if pd.isna(forecast_next):
            continue

        # Actual progression (solid)
        ax.plot([t, t_next], [actual_now, actual_next], color='blue', label='Actual' if i == 10 else "")

        # Forecasted progression (dashed)
        ax.plot([t, t_next], [actual_now, forecast_next], color='orange', linestyle='--', label='Forecast' if i == 10 else "")

    ax.set_title("Step-Ahead Forecast Visualization")
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of Crimes")
    ax.legend()
    plt.tight_layout()
    plt.savefig("your_filename.png", dpi=300, bbox_inches='tight')

    plt.show()

    print(results.summary())
