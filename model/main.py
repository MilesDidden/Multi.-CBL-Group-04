from DB_utils import DBhandler
from table_joining_utils import join_tables

from multiprocessing import Pool
import psutil
import pandas as pd
from functools import partial
import os
from time import time


CPU_COUNT = psutil.cpu_count(logical=False)


def process_chunk(offset: int, chunk_size: int, imd_data: pd.DataFrame, ward_data: pd.DataFrame):
    db_handler = DBhandler("../data/", "crime_data_UK_v3.db", verbose=0)

    crime_data = db_handler.query(
        f"""
        SELECT 
            * 
        FROM 
            crime
        ORDER BY
            crime_id
        LIMIT
            {chunk_size}
        OFFSET
            {offset}
        """, 
        False
    )
    db_handler.close_connection_db()

    df_final_temp = join_tables(crime_data=crime_data, ward_data=ward_data, imd_data=imd_data)

    return df_final_temp


if __name__ == "__main__":

    t0 = time()

    # Init db
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v3.db", verbose=0)

    crime_count = db_handler.query("SELECT COUNT(crime_id) as crimes FROM crime", True).loc[0, "crimes"]
    chunk_size = int(crime_count/CPU_COUNT)

    # Split data into chunks for querying & joining
    print(f"\nDividing computational work over {CPU_COUNT} workers.\n")
    offset_per_agent = [i for i in range(0, crime_count, chunk_size)]

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

    process_chunk_partial = partial(process_chunk, chunk_size=chunk_size, imd_data=imd_data, ward_data=ward_data)

    with Pool(CPU_COUNT) as pool:
        results = pool.map(process_chunk_partial, offset_per_agent)

    df_final = pd.concat(results, ignore_index=True, axis=0)

    print(f"Time it took to run querying & joining: {time() - t0}")

    # Temporary save file under "temp_results"
    df_final.to_csv(os.path.join(db_handler.db_loc, "temp_results.csv"))
