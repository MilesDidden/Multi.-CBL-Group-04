from DB_utils import DBhandler
from sklearn.cluster import KMeans
import plotly.graph_objects as go
import pandas as pd

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

    db_handler.close_connection_db()

    # Return centroids and full dataframe
    crime_locations["cluster"] = kmeans.labels_
    return centroids, crime_locations

if __name__ == "__main__":
    centroids, clustered_data = run_kmeans(ward_code)
    print(f"Police officers allocation for {ward_code}:\n", centroids)

    def plot_kmeans_clusters(clustered_data, centroids, ward_code):
        fig = go.Figure()

        # Add points for each cluster
        for cluster_id in clustered_data["cluster"].unique():
            cluster_points = clustered_data[clustered_data["cluster"] == cluster_id]
            fig.add_trace(go.Scattergeo(
                lon=cluster_points["longitude"],
                lat=cluster_points["latitude"],
                mode="markers",
                marker=dict(size=5),
                name=f"Cluster {cluster_id}",
                showlegend=False
            ))

        # Add centroids
        centroid_lats = centroids[:, 0]
        centroid_lons = centroids[:, 1]
        fig.add_trace(go.Scattergeo(
            lon=centroid_lons,
            lat=centroid_lats,
            mode="markers",
            marker=dict(size=10, symbol="x", color="black"),
            name="Police Officers"
        ))

        fig.update_layout(
        title=f"K-Means Clustering of Crimes in Ward {ward_code}",
        autosize=True,
        height=800,
        margin=dict(l=0, r=0, t=50, b=0),
        geo=dict(
            scope='europe',
            showland=True,
            landcolor="rgb(243, 243, 243)",
            showcountries=False,
            lataxis=dict(range=[min(clustered_data["latitude"]) - 0.01,
                                max(clustered_data["latitude"]) + 0.01]),
            lonaxis=dict(range=[min(clustered_data["longitude"]) - 0.01,
                                max(clustered_data["longitude"]) + 0.01]),
            )  
        )

        return fig

    if __name__ == "__main__":
        centroids, clustered_data = run_kmeans(ward_code)
        fig = plot_kmeans_clusters(clustered_data, centroids, ward_code)
        fig.write_html("kmeans_clusters_plot.html", auto_open=True)

    
    # Clean up: Drop temp table
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")
    drop_query = f"DROP TABLE IF EXISTS temp_crime_{ward_code};"
    db_handler.update(drop_query)
    db_handler.close_connection_db()