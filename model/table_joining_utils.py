import pandas as pd
from shapely import wkt
from shapely.geometry import Point
import geopandas as gpd

def join_tables(crime_data: pd.DataFrame, ward_data: pd.DataFrame, imd_data: pd.DataFrame) -> pd.DataFrame:

    # Compute average IMD decile per LSOA (feature_code)
    avg_imd_per_lsoa = imd_data.rename(columns={'value': 'average_imd_decile'})

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

    return result
