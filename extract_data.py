from DB_utils import DBhandler
import pandas as pd

# Functions

if __name__ == "__main__":
    db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")

    crime_data = db_handler.query(
        "SELECT * FROM crime LIMIT 100"
    ) # Loads crime data in RAM

    imd_data = db_handler.query(
        "SELECT * FROM imd_data WHERE measurement like '%Decile%' and indices_of_deprivation like '%Education%'"
    ) # Loads IMD data in RAM

    result = pd.merge(crime_data, imd_data, how="left", left_on="lsoa_code", right_on="feature_code") # Left join on lsoa codes

    db_handler.close_connection_db()

    print(result)
    