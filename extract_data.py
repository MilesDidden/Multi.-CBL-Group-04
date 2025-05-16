from DB_utils import DBhandler
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt

# Functions

if __name__ == "__main__":
    # Connect to the database
    db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")
    
    # Run the query
    ward_sample = db_handler.query("SELECT * FROM ward_location LIMIT 100", True)
    
    # Close the connection
    db_handler.close_connection_db()
    
    # Print the result
    print(ward_sample.head())


if __name__ == "__main__":
    db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")
    

    crime_data = db_handler.query(
        "SELECT * FROM crime", True
    ) # Loads crime data in RAM

    imd_data = db_handler.query(
        "SELECT * FROM imd_data WHERE measurement like '%Decile%' and indices_of_deprivation like '%Education%'", True
    ) # Loads IMD data in RAM

    result = pd.merge(crime_data, imd_data, how="left", left_on="lsoa_code", right_on="feature_code") # Left join on lsoa codes

    db_handler.close_connection_db()

    print(result)


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
    merged = merge_crime_with_education_imd(db_path="data", db_name="crime_data_UK_v2.db")
    
    # Group and summarize 
    result = merged.groupby(["lsoa_code", "month"]).agg(
        crime_count=('crime_id', 'count'),
        avg_imd_value=('value', 'mean')
    )
    
    print(result)

def assign_wards_to_crimes(db_path: str, db_name: str) -> gpd.GeoDataFrame:
    # Connect and load data
    db_handler = DBhandler(db_loc=db_path, db_name=db_name)
    
    crime_df = db_handler.query("SELECT * FROM crime", True)
    ward_df = db_handler.query("SELECT * FROM ward_location", True)
    
    db_handler.close_connection_db()

    # Drop crimes without coordinates
    crime_df = crime_df.dropna(subset=["lat", "long"])

    # Convert crimes into GeoDataFrame using lat/lon
    crime_gdf = gpd.GeoDataFrame(
        crime_df,
        geometry=[Point(xy) for xy in zip(crime_df["long"], crime_df["lat"])],
        crs="EPSG:4326"
    )

    # Convert ward WKT geometries to shapely polygons
    ward_df["geometry"] = ward_df["geometry"].apply(wkt.loads)
    ward_gdf = gpd.GeoDataFrame(ward_df, geometry="geometry", crs="EPSG:4326")

    # Spatial join: assigns each crime to the ward it falls within
    joined = gpd.sjoin(crime_gdf, ward_gdf[["ward_code", "ward_name", "geometry"]], how="left", predicate="within")

    return joined

# Example usage
if __name__ == "__main__":
    result = assign_wards_to_crimes("data", "crime_data_UK_v2.db")
    
    # Show sample with assigned wards
    print(result[["crime_id", "lat", "long", "ward_code", "ward_name"]].head())

    # Optional: save to CSV
    result.to_csv("data/crime_with_wards.csv", index=False)