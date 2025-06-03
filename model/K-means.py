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
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")

    crime_query = f"""
    SELECT lat AS latitude, long AS longitude
    FROM temp_crime_{ward_code}
    WHERE lat IS NOT NULL AND long IS NOT NULL;
    """
    crime_locations = db_handler.query(crime_query)

    if crime_locations.empty:
        raise ValueError(f"No valid lat/long entries found for ward {ward_code}")

    coords = crime_locations[["latitude", "longitude"]].to_numpy()

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(coords)
    centroids = kmeans.cluster_centers_

    db_handler.close_connection_db()

    crime_locations["cluster"] = kmeans.labels_
    return centroids, crime_locations


def plot_kmeans_clusters(clustered_data, centroids, ward_code):
    fig = go.Figure()

    # Crime cluster points
    fig.add_trace(go.Scattermapbox(
        lat=clustered_data["latitude"],
        lon=clustered_data["longitude"],
        mode="markers",
        marker=dict(
            size=8,
            color=clustered_data["cluster"],
            colorscale="Viridis",
            opacity=0.7,
            showscale=False
        ),
        name="Crime Clusters",
        hoverinfo="text",
        text=[f"Cluster {c}" for c in clustered_data["cluster"]]
    ))

    # Prepare custom hover text for centroids
    centroid_hover_texts = [
        f"Cluster {i}<br>Lat: {lat:.5f}<br>Lon: {lon:.5f}"
        for i, (lat, lon) in enumerate(centroids)
    ]

    # Police officer centroid markers
    fig.add_trace(go.Scattermapbox(
        lat=centroids[:, 0],
        lon=centroids[:, 1],
        mode="markers+text",
        marker=dict(
            size=14,
            color="black",
            symbol="x"
        ),
        name="Police Officers",
        text=["X"] * len(centroids),
        textfont=dict(size=14, color="black"),
        textposition="middle center",
        hoverinfo="text",
        hovertext=centroid_hover_texts
    ))

    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            zoom=11,
            center=dict(lat=51.5074, lon=-0.1278)
        ),
        title=f"K-Means Clustering of Crimes in Ward {ward_code}",
        autosize=True,
        height=800,
        margin=dict(l=0, r=0, t=50, b=0)
    )

    return fig


if __name__ == "__main__":
    centroids, clustered_data = run_kmeans(ward_code)
    print(f"Police officers allocation for {ward_code}:\n", centroids)
    fig = plot_kmeans_clusters(clustered_data, centroids, ward_code)
    fig.write_html("kmeans_clusters_plot.html", auto_open=True)

    # Clean up: Drop temp table
    db_handler = DBhandler(db_loc="../data/", db_name="crime_data_UK_v4.db")
    drop_query = f"DROP TABLE IF EXISTS temp_crime_{ward_code};"
    db_handler.update(drop_query)
    db_handler.close_connection_db()
