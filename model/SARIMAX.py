from DB_utils import DBhandler
import pandas as pd
import statsmodels.api as sm
import os
import matplotlib.pyplot as plt


if __name__ == "__main__":

    # Load DBhandler to get data path
    db_handler = DBhandler(db_loc="../data", db_name="crime_data_UK_v2.db")
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
    df["avg_imd"] = df["avg_imd"].interpolate()  # Interpolate exogenous variable

    print(f"Time series length: {len(df)} months")
    if len(df) < 24:
        print("Warning: Less than 2 years of data. Seasonal models may be unreliable.")

    # Fit SARIMAX model with seasonal component and exogenous variable
    sarimax = sm.tsa.statespace.SARIMAX(
        df["num_of_crimes"],
        exog=df["avg_imd"],
        order=(2, 1, 2),
        # seasonal_order=(1, 1, 0, 12),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    results = sarimax.fit(disp=False)

    # Forecast (same exog length)
    df["forecast"] = results.predict(start=10, end=len(df)-1, exog=df["avg_imd"][10:])

    # Plot
    df[["num_of_crimes", "forecast"]].plot(title="Observed vs Forecasted Crime Counts", figsize=(10, 4))
    plt.show()
    plt.savefig("temp")

    print(results.summary())
