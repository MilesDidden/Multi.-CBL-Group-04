import sqlite3
import pandas as pd
import plotly.express as px
import os
import shutil
import tempfile

from model.DB_utils import DBhandler
from model.ML_utils import create_temp_table
from model.SARIMAX import timeseries
from model.KMeans import run_kmeans_weighted, plot_kmeans_clusters


# def load_crime_data(ward_code: str, db_path: str = "data/crime_data_UK_v4.db") -> pd.DataFrame:
#     """
#     Loads burglary crime data for a given ward_code from the database.
#     """
#     query = """
#         SELECT * FROM crime
#         WHERE ward_code = ?
#         AND crime_type = 'Burglary'
#     """

#     try:
#         conn = sqlite3.connect(db_path)
#         df = pd.read_sql_query(query, conn, params=(ward_code,))
#         conn.close()
#         return df

#     except Exception as e:
#         print(f"Error loading data for ward '{ward_code}': {e}")
#         return pd.DataFrame()


def load_ward_options(db_path: str = "data/crime_data_UK_v4.db"):
    """
    Loads all ward codes and names for dropdown selection in the dashboard.
    """
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


# def get_forecast_plot_and_value(ward_code: str, db_loc: str = "data/", db_name: str = "crime_data_UK_v4.db"):
#     """
#     Runs forecasting pipeline and returns a Plotly figure and forecasted value.
#     """
#     try:
#         try:
#             db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)
#             db_handler.close_connection_db()
#         except Exception as db_connection_error:
#             print(f"❌ Failed to connect to database: {db_connection_error}")
#             print("Copying database to temporary location!")

#             abs_original_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", db_loc, db_name))
#             temp_dir = "/tmp"
#             try:
#                 os.makedirs(temp_dir, exist_ok=True)
#             except OSError as e:
#                 print(f"❌ Failed to create temp directory: {e}")
#                 return {}, "Temp storage error"
#             temp_db_path = os.path.join(temp_dir, db_name)
#             if not os.path.exists(temp_db_path):
#                 print(f"Temporary Database does not exist in current path, copying database to: {temp_db_path}")
#                 shutil.copyfile(abs_original_db_path, temp_db_path)

#             db_loc = os.path.dirname(temp_db_path)
#             db_name = os.path.basename(temp_db_path)

#         create_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

#         # main SARIMAX call
#         fig, forecast_value, imd = timeseries(ward_code=ward_code, db_loc=db_loc, db_name=db_name)
#         print(f"✅ Forecast value: {forecast_value}, IMD: {imd}")
#         return fig, forecast_value, imd
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         print(f"❌ Error generating forecast for {ward_code}: {e}")
#         return {}, "Forecast unavailable"


def check_accesiblity_db(db_loc: str="../data/", db_name: str="crime_data_UK_v4.db"):
    try:
        db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)
        db_handler.close_connection_db()
        return db_loc, db_name
        
    except Exception as db_connection_error:
        print(f"❌ Failed to connect to database: {db_connection_error}")
        print("Copying database to temporary location!")

        abs_original_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", db_loc, db_name))
        temp_dir = "/tmp"
        try:
            os.makedirs(temp_dir, exist_ok=True)
        except OSError as e:
            print(f"❌ Failed to create temp directory: {e}")
            return {}, "Temp storage error"
        temp_db_path = os.path.join(temp_dir, db_name)
        if not os.path.exists(temp_db_path):
            print(f"Temporary Database does not exist in current path, copying database to: {temp_db_path}")
            shutil.copyfile(abs_original_db_path, temp_db_path)

        db_loc = os.path.dirname(temp_db_path)
        db_name = os.path.basename(temp_db_path)
        return db_loc, db_name


def prevent_imd_from_reaching_extreme_points(imd_value):

        # Prevent weights from collapsing to 0 (which breaks np.average)
        # IMD scale is from 1 (most deprived) to 10 (least)
    if imd_value >= 9.9:
        print(f"[WARNING] IMD value {imd_value} too high — capping to 9.9 to avoid zero weights")
        imd_value = 9.9
    elif imd_value <= 0.1:
        print(f"[WARNING] IMD value {imd_value} too low — raising to 0.1 to avoid overly high weights")
        imd_value = 0.1

    return imd_value


# def get_clustered_map(ward_code: str, num_officers: int, external_forecast_value=None, imd_value=None, db_loc: str = "data/", db_name: str = "crime_data_UK_v4.db", return_df=False):

#     """
#     Generates the KMeans deployment map with officer clusters based on forecast and IMD.
#     """
#     print(f"Running get_clustered_map for ward {ward_code} with {num_officers} officers")

#     try:
#         try:
#             db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)
#             db_handler.close_connection_db()
#         except Exception as db_connection_error:
#             print(f"❌ Failed to connect to database: {db_connection_error}")
#             print("Copying database to temporary location!")

#             abs_original_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", db_loc, db_name))
#             temp_dir = "/tmp"
#             try:
#                 os.makedirs(temp_dir, exist_ok=True)
#             except OSError as e:
#                 print(f"❌ Failed to create temp directory: {e}")
#                 return {}, "Temp storage error"
#             temp_db_path = os.path.join(temp_dir, db_name)
#             if not os.path.exists(temp_db_path):
#                 print(f"Temporary Database does not exist in current path, copying database to: {temp_db_path}")
#                 shutil.copyfile(abs_original_db_path, temp_db_path)

#             db_loc = os.path.dirname(temp_db_path)
#             db_name = os.path.basename(temp_db_path)


#     try:
#         abs_original_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", db_loc, db_name))
#         temp_dir = "/tmp"
#         try:
#            os.makedirs(temp_dir, exist_ok=True)
#         except OSError as e:
#            print(f"❌ Failed to create temp directory: {e}")
#            return {}, "Temp storage error"
#         temp_db_path = os.path.join(temp_dir, f"temp_kmeans_db_{ward_code}.db")
#         shutil.copyfile(abs_original_db_path, temp_db_path)

#         temp_db_folder = os.path.dirname(temp_db_path)
#         temp_db_name = os.path.basename(temp_db_path)

#         if (external_forecast_value is None) or (imd_value is None):
#             _, forecast_value, imd_value = timeseries(ward_code=ward_code, db_loc=temp_db_folder, db_name=temp_db_name)
#             forecast_value = int(round(forecast_value))
       
#         # Prevent weights from collapsing to 0 (which breaks np.average)
#         # IMD scale is from 1 (most deprived) to 10 (least)
#         if imd_value >= 9.9:
#             print(f"[WARNING] IMD value {imd_value} too high — capping to 9.9 to avoid zero weights")
#             imd_value = 9.9
#         elif imd_value <= 0.1:
#             print(f"[WARNING] IMD value {imd_value} too low — raising to 0.1 to avoid overly high weights")
#             imd_value = 0.1

            
#         n_crimes = max(forecast_value, num_officers)

#         # new isolated try-except block just around KMeans
#         try:
#             centroids, clustered_data = run_kmeans_weighted(
#                 ward_code=ward_code,
#                 n_crimes=n_crimes,
#                 imd_value=imd_value,
#                 n_clusters=num_officers,
#                 db_loc=temp_db_folder,
#                 db_name=temp_db_name
#             )
#         except Exception as kmeans_error:
#             print(f"❌ KMeans error: {kmeans_error}")
#             import traceback
#             traceback.print_exc()
#             return px.scatter_mapbox(pd.DataFrame(), lat=[], lon=[], title="KMeans error")
        
#         print("✅ Plotting KMeans with", len(clustered_data), "points and", len(centroids), "centroids")

#         fig = plot_kmeans_clusters(clustered_data, centroids, ward_code, db_loc=temp_db_folder, db_name=temp_db_name)

#         if return_df:
#            return fig, clustered_data  
#         else:
#            return fig


#     except Exception as e:
#         print(f"Error in KMeans deployment: {e}")
#         return px.scatter_mapbox(pd.DataFrame(), lat=[], lon=[], title="Map error")
