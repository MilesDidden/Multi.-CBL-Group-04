from DB_utils import DBhandler
import pandas as pd
from shapely import wkt
from shapely.geometry import Point
import geopandas as gpd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

if __name__ == "__main__":
    # Init db
    db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")
    
    # Query data
    crime_data = db_handler.query(
        "SELECT * FROM crime", True
    )  # Loads crime data in RAM

    # Load all IMD Decile values for all domains
    imd_data = db_handler.query(
    """
    SELECT * FROM imd_data
    WHERE measurement LIKE '%Decile%'
    AND (
        indices_of_deprivation LIKE '%Income%' OR
        indices_of_deprivation LIKE '%Employment%' OR
        indices_of_deprivation LIKE '%Education%' OR
        indices_of_deprivation LIKE '%Health%' OR
        indices_of_deprivation LIKE '%Crime%' OR
        indices_of_deprivation LIKE '%Barriers%' OR
        indices_of_deprivation LIKE '%Environment%'
    )
    """,
    True
    )
    
    # Load ward polygons
    ward_data = db_handler.query(
        "SELECT * FROM ward_location", True
    )  # Loads ward data in RAM

    # Close connection
    db_handler.close_connection_db()

    # Compute average IMD decile per LSOA (feature_code)
    avg_imd_per_lsoa = (
        imd_data
        .groupby('feature_code')['value']
        .mean()
        .reset_index()
        .rename(columns={'value': 'average_imd_decile'})
    )

    # Merge average IMD with crime data
    crime_and_imd_data = pd.merge(
        crime_data, avg_imd_per_lsoa, how="left", left_on="lsoa_code", right_on="feature_code"
    )
    print("\nMerged average IMD deciles with crime data!\n")

    # Convert lat/long to Point geometry
    crime_and_imd_data['geometry'] = crime_and_imd_data.apply(lambda row: Point(row['long'], row['lat']), axis=1)
    crime_and_imd_gdf = gpd.GeoDataFrame(crime_and_imd_data, geometry='geometry', crs="EPSG:4326")

    # Convert ward geometry from WKT and reproject to match crime data CRS
    ward_data['geometry'] = ward_data['geometry'].apply(wkt.loads)
    ward_df = gpd.GeoDataFrame(ward_data, geometry='geometry', crs="EPSG:27700").to_crs(epsg=4326)

    # Spatial join: get the ward each crime falls within
    result = gpd.sjoin(crime_and_imd_gdf, ward_df[['ward_code', 'ward_name', 'geometry']], how="left", predicate="within")
    print("\nMerged wards with crime data!\n")

    print(result.head())
    result.to_csv("temp_results.csv", index=False)