import pandas as pd
import geopandas as gpd
import numpy as np
import os
import sqlite3
import json

import pandas as pd
import xml.etree.ElementTree as ET
import re
from tqdm import tqdm
import secrets
import time



def parse_kml_multipolygon(parent_path: str, kml_file: str) -> pd.DataFrame:
    """
    Parses a KML file containing a MultiPolygon and returns a Pandas DataFrame.
    Works even if namespace changes.
    """

    # Parse
    tree = ET.parse(os.path.join(parent_path, kml_file))
    root = tree.getroot()

    # Dynamically extract namespace
    m = re.match(r'\{.*\}', root.tag)
    namespace = m.group(0) if m else ''

    # Find Placemark anywhere
    placemarks = root.findall(f'.//{namespace}Placemark')
    if not placemarks:
        raise ValueError(f"No Placemark found in {kml_file}")

    # Assume one placemark (your case)
    placemark = placemarks[0]

    # Find coordinates (could be multiple)
    coordinates_list = []
    for coords_elem in placemark.findall(f'.//{namespace}coordinates'):
        coords_text = coords_elem.text.strip()
        polygon = []
        for coord in coords_text.split():
            lon, lat, *_ = coord.split(',')
            polygon.append((float(lat), float(lon)))  # (lat, lon) order
        coordinates_list.append(polygon)

    # Build DataFrame
    data = {
        'force_district_name': kml_file.replace(".kml", ""),
        'multipolygon': [coordinates_list]
    }

    df = pd.DataFrame(data)
    return df


def list_all_street_crime_csv_files(parent_path: str= "data/crime_data/") -> list[str]:
    
    csv_paths = []
    
    for dirpath, dirnames, filenames in os.walk(parent_path):
        for file in filenames:
            if file.endswith(".csv") and ("street" in file) and (("metropolitan" in file) or ("city-of-london") in file):
                csv_paths.append(os.path.join(dirpath, file))

    return csv_paths


def generate_SHA256_key(existing_keys: set[str]) -> str:
    new_key = None

    while True:
        new_key = secrets.token_hex(32)
        if new_key not in existing_keys:
            existing_keys.add(new_key)
            return new_key


def extract_and_transform_crime_data(csv_path: str, only_with_crime_ids: bool, existing_crime_ids: set) -> pd.DataFrame:
    
    df = pd.read_csv(csv_path)

    if only_with_crime_ids:
        df_filtered = df[pd.notna(df["Crime ID"])]

        temp_existing_crime_ids = df_filtered["Crime ID"].to_list()

        existing_crime_ids.update(temp_existing_crime_ids)

        return df_filtered

    else:

        df_filtered = df[pd.isna(df["Crime ID"])].copy()

        number_of_new_keys_needed = len(df_filtered)
        new_keys = [generate_SHA256_key(existing_keys=existing_crime_ids) for _ in range(number_of_new_keys_needed)]

        df_filtered["Crime ID"] = new_keys

        return df_filtered


def list_lsoa_data_files(parent_path: str= "data/LB_shp/") -> list[str]:
    shp_files = []
    for dirpath, dirnames, filenames in os.walk(parent_path):
        for file in filenames:
            if file.endswith(".shp"):
                shp_files.append(os.path.join(dirpath, file))

    return shp_files


def combine_all_lsoa_data_files(list_of_shp_file_paths: list[str]) -> pd.DataFrame:
    gdfs = []

    for file_path in list_of_shp_file_paths:
        gdfs.append(gpd.read_file(file_path))
    
    return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))


class DBhandler:

    def __init__(self, db_loc: str= 'data', db_name: str= 'crime_data_UK_v2.db') -> None:
        
        self.existing_crime_ids = set()

        self.db_loc = os.path.abspath(db_loc)
        self.db_name = db_name
        self.db_path = os.path.join(self.db_loc, self.db_name)

        print(f"{os.getcwd()}")

        if not os.path.exists(self.db_path):
            print("\nDatabase not found! Creating new database ...\n")

            try:
                self.con = sqlite3.connect(self.db_path)
                print(f"\nDatabase created at {self.db_path}\n")
            except:
                raise ValueError("\nInvalid database location!\n")
            
        else:
            self.con = sqlite3.connect(self.db_path)
            
            print("\nEstablished connection with database!\n")


    # Connect to db to update tables
    def open_connection_db(self) -> None:
        if self.con is not None:
            print("\nConnection is already open!\n")
        
        if self.db_path is None:
            raise ValueError("Make sure the database location is correct!")
        
        self.con = sqlite3.connect(self.db_path)

        print("\nConnection Opened!\n")


    # Close connection to db
    def close_connection_db(self) -> None:
        if self.con is None:
            raise ValueError("\nNo current open connections!\n")
        
        self.con.close()
        self.con = None

        print('\nConnection successfully closed!\n')


    # Create table
    def create_table(self, table_name: str, columns: dict) -> None:
        if self.con is None:
            raise ValueError("No active database connection. Open the connection first.")

        if not columns:
            raise ValueError("Columns dictionary is empty.")
        
        # Create SQL command
        columns_def = ", ".join([f"{col} {datatype}" for col, datatype in columns.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def});"

        # Execute
        cursor = self.con.cursor()
        cursor.execute(sql)
        self.con.commit()

        # Confirmation
        print(f"\nTable '{table_name}' created successfully (if non-existing).\n")


    # Delete table
    def delete_table(self, table_name: str) -> None:
        if self.con is None:
            raise ValueError("No active database connection. Open the connection first.")

        if not table_name:
            raise ValueError("Table name cannot be empty.")

        cursor = self.con.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.con.commit()

        print(f"\nTable '{table_name}' deleted successfully (if it existed).\n")


    # List existing tables
    def list_tables(self) -> list:
        if self.con is None:
            raise ValueError("No active database connection. Open the connection first.")

        cursor = self.con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Extract table names from query result
        table_list = [table[0] for table in tables]

        print("\nTables currently in database:")
        for t in table_list:
            print(f"- {t}")
        print()

        return table_list


    # Upload data to table
    def insert_rows(self, table_name: str, data: list[dict]) -> None:
        if self.con is None:
            raise ValueError("No active database connection. Open the connection first.")
        
        if not data:
            raise ValueError("No data provided to insert.")

        # Get column names from the first dictionary
        columns = data[0].keys()
        column_str = ", ".join(columns)
        placeholder_str = ", ".join([f":{col}" for col in columns])  # named style for SQLite

        # Create the SQL insert statement
        sql = f"INSERT OR IGNORE INTO {table_name} ({column_str}) VALUES ({placeholder_str})"

        # Execute insert for all rows
        cursor = self.con.cursor()
        cursor.executemany(sql, data)
        self.con.commit()

        print(f"\nInserted {len(data)} rows into '{table_name}' successfully.\n")


    # Remove duplicate rows
    def remove_duplicate_rows(self) -> None:
        pass


    # Query something
    def query(self, query_txt: str, analyze_query_time: bool= False) -> pd.DataFrame:
        
        if self.con is None:
            raise ValueError("No active database connection. Open the connection first.")
        
        if analyze_query_time:
            t0 = time.time()

        temp_df = pd.read_sql(query_txt, self.con)

        if analyze_query_time:
            print(f"\nTime it took to run the query: {(time.time()-t0):2f}")

        return temp_df
