import sqlite3
import os
import shutil

from model.DB_utils import DBhandler


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
