from DB_utils import DBhandler
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, Polygon
import geopandas as gpd

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

#Initialize DB connection
db_handler = DBhandler(db_loc="data", db_name="crime_data_UK_v2.db")

#Load ward polygons
ward_data = db_handler.query("SELECT * FROM ward_location")
ward_df = pd.DataFrame(ward_data)

#Convert WKT strings to Shapely geometry objects
ward_df['geometry'] = ward_df['geometry'].apply(wkt.loads)

#Check the first few geometries
print(ward_df[['ward_code', 'ward_name', 'geometry']].head())

#Load crimes with valid coordinates
crime_data = db_handler.query("SELECT * FROM crime WHERE lat IS NOT NULL AND long IS NOT NULL")
crime_df = pd.DataFrame(crime_data)

#Convert each lat/long to a Shapely Point
crime_df['point'] = crime_df.apply(lambda row: Point(row['long'], row['lat']), axis=1)

# Function to find matching ward
def find_ward(point, ward_df):
    for _, ward_row in ward_df.iterrows():
        if Polygon(ward_row['geometry']).contains(point):
            return pd.Series([ward_row['ward_code'], ward_row['ward_name']])
        else:
            return pd.Series([None, None])  # No match found

crime_sample = crime_df.sample(n=1000, random_state=42).copy()

# Step 2: Match each crime to a ward (only 1000 records now!)
print("Matching 1000 sampled crimes to wards...")
crime_sample[['ward_code', 'ward_name']] = crime_sample['point'].apply(lambda pt: find_ward(pt, ward_df))

# Step 3: Preview results
print(crime_sample[['crime_id', 'lat', 'long', 'ward_code', 'ward_name']])

# Step 4: Export only the sample
csv_path = "data/crime_with_wards_sample1000.csv"
crime_sample.to_csv(csv_path, index=False)
