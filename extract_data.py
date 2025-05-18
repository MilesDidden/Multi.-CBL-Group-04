from DB_utils import DBhandler
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, Polygon
import geopandas as gpd
from tqdm import tqdm


#LSOAS ON IMD
def merge_crime_with_education_imd(db_path: str, db_name: str) -> pd.DataFrame:
    # Initialize DB handler
    db_handler = DBhandler(db_loc=db_path, db_name=db_name)
    
    # Load crime data
    crime_data = db_handler.query("SELECT * FROM crime", True)

    # Load education-related IMD decile data
    imd_query = """
    SELECT * FROM imd_data 
    WHERE measurement LIKE '%Decile%' 
    AND indices_of_deprivation LIKE '%Education%'
    """
    imd_data = db_handler.query(imd_query, True)

    # Close the DB connection
    db_handler.close_connection_db()

    # Merge on LSOA code (left join)
    merged_df = pd.merge(
        crime_data, 
        imd_data, 
        how="left", 
        left_on="lsoa_code", 
        right_on="feature_code"
    )

    return merged_df


if __name__ == "__main__":
    # db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")
    

    # crime_data = db_handler.query(
    #     "SELECT * FROM crime", True
    # ) # Loads crime data in RAM

    # imd_data = db_handler.query(
    #     "SELECT * FROM imd_data WHERE measurement like '%Decile%' and indices_of_deprivation like '%Education%'", True
    # ) # Loads IMD data in RAM

    # result = pd.merge(crime_data, imd_data, how="left", left_on="lsoa_code", right_on="feature_code") # Left join on lsoa codes

    # db_handler.close_connection_db()

    # print(result)



    # merged = merge_crime_with_education_imd(db_path="data", db_name="crime_data_UK_v2.db")
    
    # # Group and summarize 
    # result = merged.groupby(["lsoa_code", "month"]).agg(
    #     crime_count=('crime_id', 'count'),
    #     avg_imd_value=('value', 'mean')
    # )
    
    # print(result)

    #Initialize DB connection
    db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")

    #Load ward polygons
    ward_data = db_handler.query("SELECT * FROM ward_location")

    #Convert WKT strings to Shapely geometry objects
    ward_data['geometry'] = ward_data['geometry'].apply(wkt.loads)

    ward_df = gpd.GeoDataFrame(ward_data, geometry='geometry')
    ward_df.set_crs(epsg=4326, inplace=True)

    #Check the first few geometries
    print(ward_df[['ward_code', 'ward_name', 'geometry']].head())

    #Load crimes with valid coordinates
    crime_data = db_handler.query("SELECT * FROM crime WHERE lat IS NOT NULL AND long IS NOT NULL LIMIT 1000")
    crime_df = crime_data.copy()

    #Convert each lat/long to a Shapely Point
    crime_df['point'] = crime_df.apply(lambda row: Point(row['long'], row['lat']), axis=1)

    crime_gdf = gpd.GeoDataFrame(crime_df, geometry='point')
    crime_gdf.set_crs(epsg=4326, inplace=True)

    # # Function to find matching ward
    # def find_ward(point, ward_df):
    #     for _, ward_row in tqdm(ward_df.iterrows()):
    #         if ward_row['geometry'].contains(point):
    #             return pd.Series([ward_row['ward_code'], ward_row['ward_name']])
    #         else:
    #             return pd.Series([None, None])  # No match found

    # crime_sample = crime_df.copy()
    
    joined = gpd.sjoin(ward_df[['ward_code', 'ward_name', 'geometry']], crime_gdf, how="left", predicate="within")

    print(joined.head())

    # # Step 2: Match each crime to a ward (trying for 1000 samples for now)
    # print("Matching 1000 sampled crimes to wards...")
    # crime_sample[['ward_code', 'ward_name']] = crime_sample['point'].apply(lambda pt: find_ward(pt, ward_df))
    # # Step 3: Preview results
    # print(crime_sample[['crime_id', 'lat', 'long', 'ward_code', 'ward_name']])
    # # Step 4: Export
    # csv_path = "data/crime_with_wards_sample1000.csv"
    # crime_sample.to_csv(csv_path, index=False)
