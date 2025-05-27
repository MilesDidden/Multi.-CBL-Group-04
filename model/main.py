from DB_utils import DBhandler
from table_joining_utils import join_tables

from multiprocessing import Pool
import psutil
import pandas as pd
import os
from time import time

CPU_COUNT = psutil.cpu_count(logical=False)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = "../data/"
DB_NAME = "crime_data_UK_v3.db"

# Temp paths for parquet files
IMD_PARQUET = os.path.join(os.path.abspath(os.path.join(SCRIPT_DIR, DB_PATH)), "imd_data_temp.parquet")
WARD_PARQUET = os.path.join(os.path.abspath(os.path.join(SCRIPT_DIR, DB_PATH)), "ward_data_temp.parquet")


def process_chunk(offset: int, chunk_size: int):
    # Load IMD and ward data inside each process from Parquet
    imd_data = pd.read_parquet(IMD_PARQUET)
    ward_data = pd.read_parquet(WARD_PARQUET)

    db_handler = DBhandler(DB_PATH, DB_NAME, verbose=0)
    crime_data = db_handler.query(
        f"""
        SELECT
            crime_id,
            month,
            long,
            lat,
            lsoa_code
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

    # Load data once and save to Parquet for multiprocessing
    db_handler = DBhandler(DB_PATH, DB_NAME, verbose=0)
    crime_count = db_handler.query("SELECT COUNT(crime_id) as crimes FROM crime", True).loc[0, "crimes"]
    chunk_size = int(crime_count / CPU_COUNT)

    print(f"\nDividing computational work over {CPU_COUNT} workers.\n")
    offset_per_agent = [i for i in range(0, crime_count, chunk_size)]

    imd_data = db_handler.query("""
        SELECT * FROM imd_data
        WHERE measurement LIKE '%Decile%'
        AND indices_of_deprivation LIKE '%Index of Multiple Deprivation (IMD)%'
    """, True)
    imd_data.to_parquet(IMD_PARQUET, index=False)

    ward_data = db_handler.query("SELECT * FROM ward_location", True)
    ward_data.to_parquet(WARD_PARQUET, index=False)

    db_handler.close_connection_db()

    # Use Pool with starmap and arguments (offset, chunk_size)
    with Pool(CPU_COUNT) as pool:
        results = pool.starmap(process_chunk, [(offset, chunk_size) for offset in offset_per_agent])

    df_final = pd.concat(results, ignore_index=True)

    print(f"\nTime it took to run querying & joining: {time() - t0:.2f} seconds\n")
    df_final.to_csv(os.path.join(os.path.abspath(os.path.join(SCRIPT_DIR, DB_PATH)), "temp_results.csv"))

    # Clean up temporary Parquet files
    if os.path.exists(IMD_PARQUET):
        os.remove(IMD_PARQUET)
    if os.path.exists(WARD_PARQUET):
        os.remove(WARD_PARQUET)
