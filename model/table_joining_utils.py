import pandas as pd
from shapely import wkt
from shapely.geometry import Point
import geopandas as gpd


def join_tables(crime_data: pd.DataFrame, ward_data: pd.DataFrame, imd_data: pd.DataFrame) -> gpd.GeoDataFrame:
    # Step 1: Rename IMD column and merge (already fast)
    avg_imd_per_lsoa = imd_data.rename(columns={'value': 'average_imd_decile'})
    crime_and_imd_data = crime_data.merge(
        avg_imd_per_lsoa[['feature_code', 'average_imd_decile']],
        how="left",
        left_on="lsoa_code",
        right_on="feature_code"
    )

    # Step 2: Vectorized Point creation
    crime_and_imd_data['geometry'] = gpd.points_from_xy(crime_and_imd_data['long'], crime_and_imd_data['lat'])
    crime_and_imd_gdf = gpd.GeoDataFrame(crime_and_imd_data, geometry='geometry', crs="EPSG:4326")

    # Step 3: Load ward geometries from WKT and reproject
    ward_data['geometry'] = ward_data['geometry'].map(wkt.loads)
    ward_gdf = gpd.GeoDataFrame(ward_data, geometry='geometry', crs="EPSG:27700").to_crs("EPSG:4326")

    # Step 4: Spatial index for faster spatial join
    ward_gdf.sindex  # build spatial index (implicitly used by sjoin)
    result = gpd.sjoin(
        crime_and_imd_gdf,
        ward_gdf[['ward_code', 'ward_name', 'geometry']],
        how="left",
        predicate="within"
    )
    
    return result

