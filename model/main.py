from DB_utils import DBhandler
from table_joining_utils import join_tables

import os

if __name__ == "__main__":
    # Init db
    db_handler = DBhandler(db_loc="../data", db_name="crime_data_UK_v2.db")
    
    # Load crime data
    crime_data = db_handler.query(
        """
        SELECT 
            * 
        FROM 
            crime
        """, 
        True
    )

    # Load all IMD Decile values
    imd_data = db_handler.query(
        """
        SELECT 
            * 
        FROM 
            imd_data
        WHERE 
            measurement LIKE '%Decile%'
            AND indices_of_deprivation LIKE '%Index of Multiple Deprivation (IMD)%'
        """,
        True
    )
    
    # Load ward polygons
    ward_data = db_handler.query(
        """
        SELECT 
            * 
        FROM 
            ward_location
        """, 
        True
    )

    # Close connection
    db_handler.close_connection_db()

    # Join tables
    df_final = join_tables(crime_data=crime_data, ward_data=ward_data, imd_data=imd_data)

    # Show a sample of joined dataframes
    print(df_final.head())

    # Temporary save file under "temp_results"
    df_final.to_csv(os.path.join(db_handler.db_loc, "temp_results.csv"))
