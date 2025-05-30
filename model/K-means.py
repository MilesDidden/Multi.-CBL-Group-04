from DB_utils import DBhandler
from sklearn.cluster import KMeans

ward_code = "E05000138"
n_officers = 100

db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")

# Create a temporary table for the selected ward
create_temp_table_query = f"""
        CREATE TABLE IF NOT EXISTS temp_crime_{ward_code} AS
        SELECT * FROM crime
        WHERE ward_code = '{ward_code}';
    """
db_handler.update(create_temp_table_query) 


db_handler.close_connection_db()

def run_kmeans(ward_code: str, n_clusters: int = 100):
    # Initialize DB connection
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")

    # Query lat, long from temp table
    crime_query = f"""
    SELECT lat AS latitude, long AS longitude
    FROM temp_crime_{ward_code}
    WHERE lat IS NOT NULL AND long IS NOT NULL;
    """
    crime_locations = db_handler.query(crime_query)

    if crime_locations.empty:
        raise ValueError(f"No valid lat/long entries found for ward {ward_code}")

    # Convert to NumPy array for clustering
    coords = crime_locations[["latitude", "longitude"]].to_numpy()

    # KMeans clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(coords)
    centroids = kmeans.cluster_centers_

    # Clean up: Drop temp table
    drop_query = f"DROP TABLE IF EXISTS temp_crime_{ward_code};"
    db_handler.update(drop_query)

    db_handler.close_connection_db()

    # Return centroids and full dataframe
    crime_locations["cluster"] = kmeans.labels_
    return centroids, crime_locations

if __name__ == "__main__":
    centroids, clustered_data = run_kmeans(ward_code)
    print(f"Police officers allocation for {ward_code}:\n", centroids)
