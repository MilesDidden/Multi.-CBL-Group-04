from DB_utils import DBhandler
from sklearn.cluster import KMeans
import plotly.graph_objects as go

def run_kmeans(ward_code: str, n_crimes: int, n_clusters: int = 100, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db"):
    # Initialize DB connection
    db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)
    # Query lat, long from temp table
    crime_query = f"""
    SELECT 
        lat AS latitude, 
        long AS longitude
    FROM 
        temp_crime_{ward_code}
    WHERE 
        lat IS NOT NULL 
        AND long IS NOT NULL
    ORDER BY 
        RANDOM()
    LIMIT 
        {int(n_crimes)};
    """

    crime_locations = db_handler.query(crime_query)

    db_handler.close_connection_db()

    if crime_locations.empty:
        raise ValueError(f"No valid lat/long entries found for ward {ward_code}")

    # Convert to NumPy array for clustering
    coords = crime_locations[["latitude", "longitude"]].to_numpy()

    # KMeans clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(coords)
    centroids = kmeans.cluster_centers_

    # Return centroids and full dataframe
    crime_locations["cluster"] = kmeans.labels_
    
    return centroids, crime_locations



def plot_kmeans_clusters(clustered_data, centroids, ward_code, db_loc="../data/", db_name="crime_data_UK_v4.db"):

    fig = go.Figure()

    # Crime cluster points
    fig.add_trace(go.Scattermapbox(
        lat=clustered_data["latitude"],
        lon=clustered_data["longitude"],
        mode="markers",
        marker=dict(
            size=10,
            color=clustered_data["cluster"],
            colorscale="Viridis",
            opacity=0.85,
            showscale=False
        ),
        name="Crime Clusters",
        hoverinfo="text",
        text=[f"Cluster {c}" for c in clustered_data["cluster"]]
    ))

    # Prepare custom hover text for police officers
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
            size=20,
            color="red",
            symbol="cross",
        ),
        name="Police Officers",
        text=["X"] * len(centroids),
        textfont=dict(size=18, color="black"),
        textposition="middle center",
        hoverinfo="text",
        hovertext=centroid_hover_texts
    ))

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            zoom=11,
            center=dict(lat=51.5074, lon=-0.1278)
        ),
        title=f"K-Means Clustering of Crimes in Ward {ward_code}",
        autosize=True,
        height=800,
        margin=dict(l=0, r=0, t=50, b=0)
    )

    return fig
