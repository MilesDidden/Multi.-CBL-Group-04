import sqlite3
import pandas as pd


def load_crime_data(ward_code: str, db_path: str = "data/crime_data_UK_v4.db") -> pd.DataFrame:
    """
    Loads burglary crime data for a given ward_code from the database.
    """
    query = f"""
        SELECT * FROM crime
        WHERE ward_code = ?
        AND crime_type = 'Burglary'
    """

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn, params=(ward_code,))
        conn.close()
        return df

    except Exception as e:
        print(f"Error loading data for ward '{ward_code}': {e}")
        return pd.DataFrame()

from model.ML_utils import create_temp_table
from model.SARIMAX import timeseries
import os
import shutil
import tempfile


def get_forecast_plot_and_value(ward_code: str, db_loc: str = "data/", db_name: str = "crime_data_UK_v4.db"):
    try:
        # Create a temporary writable copy of the database
        abs_original_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", db_loc, db_name))
        temp_dir = tempfile.gettempdir()
        temp_db_path = os.path.join(temp_dir, f"temp_forecast_db_{ward_code}.db")

        shutil.copyfile(abs_original_db_path, temp_db_path)
        
        # Debug lines
        print(f"Temp DB copied to → {temp_db_path}")
        print(f"Writable? → {os.access(temp_db_path, os.W_OK)}")

        # Now use the copied DB for read/write access
        temp_db_folder = os.path.dirname(temp_db_path)
        temp_db_name = os.path.basename(temp_db_path)

        # Create temp table and run forecast
        create_temp_table(ward_code=ward_code, db_loc=temp_db_folder, db_name=temp_db_name)
        fig, forecast_value = timeseries(ward_code=ward_code, db_loc=temp_db_folder, db_name=temp_db_name)

        return fig, forecast_value

    except Exception as e:
        print(f"Error generating forecast for {ward_code}: {e}")
        return {}, "Forecast unavailable"


def load_ward_options(db_path: str = "data/crime_data_UK_v4.db"):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT ward_code, ward_name FROM ward_location")
        rows = cursor.fetchall()
        conn.close()

        return [{"label": name, "value": code} for code, name in rows]

    except Exception as e:
        print(f"Error loading ward options: {e}")
        return []


from model.ML_utils import create_temp_table
from model.KMeans import run_kmeans
import plotly.express as px



from model.KMeans import run_kmeans, plot_kmeans_clusters  

def get_clustered_map(ward_code: str, num_officers: int, external_forecast_value=None, db_loc: str = "data/", db_name: str = "crime_data_UK_v4.db"):
    print(f"Running get_clustered_map for ward {ward_code} with {num_officers} officers")

    try:
        # Copy DB to temp
        abs_original_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", db_loc, db_name))
        temp_dir = tempfile.gettempdir()
        temp_db_path = os.path.join(temp_dir, f"temp_kmeans_db_{ward_code}.db")
        shutil.copyfile(abs_original_db_path, temp_db_path)

        temp_db_folder = os.path.dirname(temp_db_path)
        temp_db_name = os.path.basename(temp_db_path)

        create_temp_table(ward_code=ward_code, db_loc=temp_db_folder, db_name=temp_db_name)

        # Use external forecast value if provided, otherwise call timeseries
        if external_forecast_value is not None:
            forecast_value = int(round(external_forecast_value))
        else:
            _, forecast_value = timeseries(ward_code=ward_code, db_loc=temp_db_folder, db_name=temp_db_name)
            forecast_value = int(round(forecast_value))

        n_crimes = max(forecast_value, num_officers)

        centroids, clustered_data = run_kmeans(
            ward_code=ward_code,
            n_crimes=n_crimes,
            n_clusters=num_officers,
            db_loc=temp_db_folder,
            db_name=temp_db_name
        )

        return plot_kmeans_clusters(clustered_data, centroids, ward_code)

    except Exception as e:
        print(f"Error in KMeans deployment: {e}")
        return {}
