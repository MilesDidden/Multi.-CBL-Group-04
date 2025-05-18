from DB_utils import DBhandler
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, Polygon
import geopandas as gpd
from tqdm import tqdm
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

if __name__ == "__main__":
    # Init db
    db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")
    
    # Query data
    crime_data = db_handler.query(
        "SELECT * FROM crime", True
    ) # Loads crime data in RAM

    imd_data = db_handler.query(
        "SELECT * FROM imd_data WHERE measurement like '%Decile%' and indices_of_deprivation like '%Education%'", True
    ) # Loads IMD data in RAM

    #Load ward polygons
    ward_data = db_handler.query(
        "SELECT * FROM ward_location", True
    ) # Loads ward data in RAM

    # Close connection
    db_handler.close_connection_db()

    # Merge imd & crime data
    crime_and_imd_data = pd.merge(crime_data, imd_data, how="left", left_on="lsoa_code", right_on="feature_code") # Left join on lsoa codes

    # Convert crime lat/long to Point geometry
    crime_and_imd_data['geometry'] = crime_and_imd_data.apply(lambda row: Point(row['long'], row['lat']), axis=1)
    crime_and_imd_gdf = gpd.GeoDataFrame(crime_and_imd_data, geometry='geometry', crs="EPSG:4326")

    # Ensure ward geometry is also parsed from WKT and set to EPSG:4326
    ward_data['geometry'] = ward_data['geometry'].apply(wkt.loads)
    ward_df = gpd.GeoDataFrame(ward_data, geometry='geometry', crs="EPSG:27700").to_crs(epsg=4326)

    # # Spatial join: get the ward each crime is within
    result = gpd.sjoin(crime_and_imd_gdf, ward_df[['ward_code', 'ward_name', 'geometry']], how="left", predicate="within")

    print(result.head())
