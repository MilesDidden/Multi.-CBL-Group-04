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


from model.SARIMAX import timeseries

def load_forecast_plot(ward_code):
    try:
        fig, forecast_value = timeseries(ward_code)
        return fig, forecast_value
    except Exception as e:
        print(f"Error generating forecast plot: {e}")
        return None, None



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
