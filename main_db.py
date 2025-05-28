from model.DB_utils import *
from model.table_joining_utils import join_tables

from shapely.wkt import dumps as wkt_dumps
from tqdm import tqdm
import psutil
from multiprocessing import Pool
import pandas as pd


def process_chunk(offset: int, chunk_size: int, imd_parquet_loc: str, ward_parquet_loc: str) -> None:
    # Load IMD and ward data inside each process from Parquet
    imd_data = pd.read_parquet(imd_parquet_loc)
    ward_data = pd.read_parquet(ward_parquet_loc)

    db_handler = DBhandler(db_loc="../data", db_name="crime_data_UK_v3.db", verbose=0)
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

    df_final_temp = join_tables(crime_data=crime_data, ward_data=ward_data, imd_data=imd_data)

    db_handler.insert_rows("crime_temp", data=df_final_temp.to_dict(orient='records'))

    db_handler.close_connection_db()


if __name__ == "__main__":

    # Establish connection
    db_handler = DBhandler(db_loc="../data", db_name="crime_data_UK_v3.db")

    # # Create table force_districts
    # db_handler.create_table(
    #     table_name='force_districts',
    #     columns={'force_district_name': 'TEXT PRIMARY KEY',
    #                 'multipolygon':'TEXT'
    #                 }
    # )

    # # Preprocess data for force_districts
    # path_to_district_kml_files = "data/force_kmls/"
    # list_of_district_kmls = os.listdir(path_to_district_kml_files)

    # df_polygons_of_districts = []
    # for district_kml in list_of_district_kmls:
    #     df_polygons_of_districts.append(parse_kml_multipolygon(parent_path=path_to_district_kml_files, kml_file=district_kml))

    # df_districts = pd.concat(df_polygons_of_districts, ignore_index=True)
    # df_districts.multipolygon = df_districts.multipolygon.apply(lambda x: json.dumps(x))

    # # Insert data into force_districts
    # db_handler.insert_rows(
    #     table_name='force_districts',
    #     data=df_districts.to_dict(orient='records')
    # )

    # # Create table crime data
    # db_handler.create_table(
    #     table_name='crime',
    #     columns={'crime_id':'TEXT PRIMARY KEY',
    #                 'month':'TEXT',
    #                 'reported_by':'TEXT',
    #                 'falls_within':'TEXT',
    #                 'long':'REAL',
    #                 'lat':'REAL',
    #                 'location':'TEXT',
    #                 'lsoa_code':'TEXT',
    #                 'crime_type':'TEXT',
    #                 'last_outcome_category':'TEXT'
    #                 }
    # )

    # for file in tqdm(list_all_street_crime_csv_files()):
    #     temp_df = extract_and_transform_crime_data(file, True, db_handler.existing_crime_ids)

    #     if not temp_df.empty:

    #         temp_df = temp_df.drop(columns=["LSOA name", "Context"])

    #         temp_df.columns = temp_df.columns.str.strip().str.lower()

    #         temp_df = temp_df.rename(
    #             columns={
    #                 "crime id": "crime_id",
    #                 "month": "month",
    #                 "reported by": "reported_by",
    #                 "falls within": "falls_within",
    #                 "longitude": "long",
    #                 "latitude": "lat",
    #                 "location":"location",
    #                 "lsoa code": "lsoa_code",
    #                 "crime type":"crime_type",
    #                 "last outcome category":"last_outcome_category"
    #             }
    #         )
    #         # print(temp_df.head())
            
    #         db_handler.insert_rows(
    #             table_name='crime',
    #             data=temp_df.to_dict(orient='records')
    #         )
        
    #     else:
    #         print("Empty dataframe, skipping file ...")

    # print("\nInserted all crime data with crime ids!\n")

    # for file in tqdm(list_all_street_crime_csv_files()):
    #     temp_df = extract_and_transform_crime_data(file, False, db_handler.existing_crime_ids)

    #     if not temp_df.empty:

    #         temp_df = temp_df.drop(columns=["LSOA name", "Context"])

    #         temp_df.columns = temp_df.columns.str.strip().str.lower()

    #         temp_df = temp_df.rename(
    #             columns={
    #                 "crime id": "crime_id",
    #                 "month": "month",
    #                 "reported by": "reported_by",
    #                 "falls within": "falls_within",
    #                 "longitude": "long",
    #                 "latitude": "lat",
    #                 "location":"location",
    #                 "lsoa code": "lsoa_code",
    #                 "crime type":"crime_type",
    #                 "last outcome category":"last_outcome_category"
    #             }
    #         )
            
    #         db_handler.insert_rows(
    #             table_name='crime',
    #             data=temp_df.to_dict(orient='records')
    #         )

    #     else:
    #         print("Empty dataframe, skipping this file ...")

    # print("\nInserted all data with generated crime ids!\n")

    # db_handler.create_table("existing_crime_ids", columns={
    #     "existing_crime_id": "TEXT PRIMARY KEY"
    # })

    # db_handler.insert_rows("existing_crime_ids", data=[{"existing_crime_id":i} for i in db_handler.existing_crime_ids])

    # # Extract & transform lsoa data
    # lsoa_df = combine_all_lsoa_data_files(list_lsoa_data_files())

    # lsoa_df = lsoa_df[["lsoa21cd", "lsoa21nm", "geometry"]].rename(columns={
    #     "lsoa21cd":"lsoa_code",
    #     "lsoa21nm":"lsoa_name"
    # })

    # lsoa_df["geometry"] = lsoa_df["geometry"].apply(wkt_dumps)

    # # Create lsoa table
    # db_handler.create_table("lsoa_location", columns={
    #     'lsoa_code':'TEXT PRIMARY KEY',
    #     'lsoa_name':'TEXT',
    #     'geometry':'TEXT'
    # })

    # # Insert LSOA data
    # db_handler.insert_rows("lsoa_location", data=lsoa_df.to_dict(orient='records'))


    # # Extract & transform ward data
    # ward_df = gpd.read_file("data/Wards_December_2016_Boundaries_UK_BFE_2022_-5810284385438997272")
    # ward_df = ward_df[["WD16CD", "WD16NM", "geometry"]].rename(columns={
    #     "WD16CD":"ward_code",
    #     "WD16NM":"ward_name"
    # })
    # ward_df["geometry"] = ward_df["geometry"].apply(wkt_dumps)


    # # Create ward table
    # db_handler.create_table("ward_location", columns={
    #     'ward_code':'TEXT PRIMARY KEY',
    #     'ward_name':'TEXT',
    #     'geometry':'TEXT'
    # })

    # # Insert ward data
    # db_handler.insert_rows("ward_location", data=ward_df.to_dict(orient="records"))

    # #Extract & transform IMD data

    # db_handler.delete_table("imd_data")

    # imd_df = pd.read_csv("data/imd2019lsoa.csv").reset_index()
    # imd_df = imd_df[["FeatureCode", "Measurement", "Value", "Indices of Deprivation"]].rename(columns={
    #     "index":"uuid_imd",
    #     "FeatureCode":"feature_code",
    #     "Measurement":"measurement",
    #     "Value":"value",
    #     "Indices of Deprivation":"indices_of_deprivation"
    # })

    # # Create IMD table
    # db_handler.create_table("imd_data", columns={
    #     'uuid_imd':'INTEGER PRIMARY KEY',
    #     'feature_code':'TEXT',
    #     'measurement':'TEXT',
    #     'value':'REAL',
    #     'indices_of_deprivation':'TEXT'
    # })

    # # Insert imd data
    # db_handler.insert_rows("imd_data", data=imd_df.to_dict(orient="records"))


    # #### Add covid variables ####

    # db_handler.update(
    #     '''
    #     ALTER TABLE crime
    #     ADD COLUMN stringency_index REAL
    #     ''')
    # db_handler.update(
    #     '''
    #     ALTER TABLE crime
    #     ADD COLUMN covid_indicator REAL
    #     '''
    # )
    
    # min_max_month_crimes = db_handler.query(
    #     '''
    #     SELECT 
    #         MIN(month) as min_month, 
    #         MAX(month) as max_month
    #     FROM 
    #         crime
    #     '''
    # )
    # min_month_crime = min_max_month_crimes.loc[0, "min_month"]
    # max_month_crime = min_max_month_crimes.loc[0, "max_month"]

    # month_range = pd.date_range(start=min_month_crime, end=max_month_crime, freq="MS")

    # month_df = pd.DataFrame({
    #     "month": month_range.strftime("%Y-%m"),
    # })

    # covid_df = read_and_transform_stringency_data(os.path.join(db_handler.db_loc, "OxCGRT_timeseries_StringencyIndex_v1.csv"))

    # final_covid_data = month_df.merge(covid_df, on="month", how="left")

    # final_covid_data[["stringency_index", "covid_indicator"]] = final_covid_data[["stringency_index", "covid_indicator"]].fillna(0)

    # # Create temp covid table
    # db_handler.create_table("temp_covid_table", columns={
    #     "month":"TEXT PRIMARY KEY",
    #     "stringency_index":"REAL",
    #     "covid_indicator":"REAL"
    # })

    # db_handler.insert_rows("temp_covid_table", data=final_covid_data.to_dict(orient="records"))

    # db_handler.update(
    #     '''
    #     UPDATE crime
    #     SET stringency_index = (
    #         SELECT temp.stringency_index
    #         FROM temp_covid_table temp
    #         WHERE temp.month = crime.month
    #     ),
    #     covid_indicator = (
    #         SELECT temp.covid_indicator
    #         FROM temp_covid_table temp
    #         WHERE temp.month = crime.month
    #     )
    #     ''')
    
    # db_handler.delete_table('temp_covid_table')

    #### Update crime table, such that it contains imd data & ward code ####
    cpu_count = psutil.cpu_count(logical=False)

    # Temp paths for parquet files
    imd_parquet = os.path.join(db_handler.db_loc, "imd_data_temp.parquet")
    ward_parquet = os.path.join(db_handler.db_loc, "ward_data_temp.parquet")


    # Load data once and save to Parquet for multiprocessing
    crime_count = db_handler.query("SELECT COUNT(crime_id) as crimes FROM crime", True).loc[0, "crimes"]
    chunk_size = int(crime_count / cpu_count)

    print(f"\nDividing computational work over {cpu_count} workers.\n")
    offset_per_agent = [i for i in range(0, crime_count, chunk_size)]

    imd_data = db_handler.query("""
        SELECT * FROM imd_data
        WHERE measurement LIKE '%Decile%'
        AND indices_of_deprivation LIKE '%Index of Multiple Deprivation (IMD)%'
    """, True)
    imd_data.to_parquet(imd_parquet, index=False)

    ward_data = db_handler.query("SELECT * FROM ward_location", True)
    ward_data.to_parquet(ward_parquet, index=False)

    db_handler.create_table("crime_temp", columns={
        'crime_id':'TEXT PRIMARY KEY',
        'month':'TEXT',
        'reported_by':'TEXT',
        'falls_within':'TEXT',
        'long':'REAL',
        'lat':'REAL',
        'location':'TEXT',
        'lsoa_code':'TEXT',
        'crime_type':'TEXT',
        'last_outcome_category':'TEXT',
        'average_imd_decile': 'REAL',
        'ward_code': 'TEXT',
        'covid_indicator': 'REAL',
        'stringency_index': 'REAL'
    })

    # Use Pool with starmap and arguments (offset, chunk_size)
    with Pool(cpu_count) as pool:
        pool.starmap(process_chunk, [(offset, chunk_size, imd_parquet, ward_parquet) for offset in offset_per_agent])

    # Clean up temporary Parquet files
    if os.path.exists(imd_parquet):
        os.remove(imd_parquet)
    if os.path.exists(ward_parquet):
        os.remove(ward_parquet)

    #### Create index on ward_code ####
    db_handler.update("CREATE INDEX IF NOT EXISTS idx_crime_ward_code ON crime(ward_code)")

    # Close Connection
    db_handler.close_connection_db()
