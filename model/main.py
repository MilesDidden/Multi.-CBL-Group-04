from DB_utils import DBhandler
from table_joining_utils import join_tables
from time import time

import os

if __name__ == "__main__":

    # Split data into chunks for joining & 
    offset_per_agent = [i for i in range(0, 15000000, 2000000)]

    # Init db
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v3.db")
    
    # Load crime data
    crime_data = db_handler.query(
        """
        SELECT 
            * 
        FROM 
            crime
        ORDER BY
            crime_id
        LIMIT
            100000
        OFFSET
            0
        """, 
        True
    )

    # # Load all IMD Decile values
    # imd_data = db_handler.query(
    #     """
    #     SELECT 
    #         * 
    #     FROM 
    #         imd_data
    #     WHERE 
    #         measurement LIKE '%Decile%'
    #         AND indices_of_deprivation LIKE '%Index of Multiple Deprivation (IMD)%'
    #     """,
    #     True
    # )
    
    # # Load ward polygons
    # ward_data = db_handler.query(
    #     """
    #     SELECT 
    #         * 
    #     FROM 
    #         ward_location
    #     """, 
    #     True
    # )

    # Close connection
    db_handler.close_connection_db()

    # t0 = time()
    # # Join tables
    # df_final = join_tables(crime_data=crime_data, ward_data=ward_data, imd_data=imd_data)
    # print(f"took {time() - t0} seconds to join data!")

    # # Show a sample of joined dataframes
    # print(df_final.head())

    # # Temporary save file under "temp_results"
    # df_final.to_csv(os.path.join(db_handler.db_loc, "temp_results.csv"))
