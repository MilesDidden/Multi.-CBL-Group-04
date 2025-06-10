from DB_utils import DBhandler
import plotly.graph_objects as go
from shapely import wkt
import geopandas as gpd
import numpy as np
from sklearn.metrics import pairwise_distances_argmin
import pandas as pd
from geopy.distance import geodesic


def run_kmeans_weighted(ward_code: str, n_crimes: int, imd_value: float, n_clusters: int = 100, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db"):
    db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)

    # Query lat/long from temp crime table
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

    coords = crime_locations[["latitude", "longitude"]].to_numpy()

    # Weighted K-means Core Logic
    # Invert IMD so that lower deprivation score → higher weight
    ward_weight = 10 - imd_value
    weights = np.full(coords.shape[0], ward_weight)


    # Initialize centroids randomly
    rng = np.random.default_rng(42)
    initial_idxs = rng.choice(coords.shape[0], n_clusters, replace=False)
    centroids = coords[initial_idxs]

    max_iter = 100
    tol = 1e-4

    for _ in range(max_iter):
        # Step 1: Assign points to nearest centroid
        labels = pairwise_distances_argmin(coords, centroids)

        # Step 2: Update centroids with weights
        new_centroids = np.zeros_like(centroids)
        for i in range(n_clusters):
            mask = labels == i
            if not np.any(mask):
                # Reinitialize empty clusters
                new_centroids[i] = coords[rng.choice(coords.shape[0])]
                continue
            cluster_points = coords[mask]
            cluster_weights = weights[mask]
            new_centroids[i] = np.average(cluster_points, axis=0, weights=cluster_weights)

        # Step 3: Check convergence
        if np.linalg.norm(new_centroids - centroids) < tol:
            break
        centroids = new_centroids

    crime_locations["cluster"] = labels
    return centroids, crime_locations


def plot_kmeans_clusters(clustered_data, centroids, ward_code, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db"):

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
    text=[f"Cluster {c}" for c in clustered_data["cluster"]],
    hoverinfo="text"
    ))


    # Prepare centroid hover text
    centroid_hover_texts = [
        f"Cluster {i}<br>Lat: {lat:.5f}<br>Lon: {lon:.5f}"
        for i, (lat, lon) in enumerate(centroids)
    ]

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
    
    # Connect to the database
    db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)

    # Query WKT geometry from the ward geometry table (adjust table name if needed)
    query = f"""
    SELECT ward_code, geometry
    FROM ward_location
    WHERE ward_code = '{ward_code}'
    """

    ward_geom_df = db_handler.query(query)
    db_handler.close_connection_db()

    # Convert WKT to Shapely geometry
    geom = wkt.loads(ward_geom_df["geometry"].iloc[0])

    # Wrap in GeoSeries and reproject
    ward_geom = gpd.GeoSeries([geom], crs="EPSG:27700").to_crs("EPSG:4326")
    geom = ward_geom.iloc[0]  # Get reprojected geometry

    # Extract coordinates
    boundary_coords = list(geom.exterior.coords)
    boundary_lon = [lon for lon, _ in boundary_coords]
    boundary_lat = [lat for _, lat in boundary_coords]

    fig.add_trace(go.Scattermapbox(
        lat=boundary_lat + [boundary_lat[0]],
        lon=boundary_lon + [boundary_lon[0]],
        mode="lines",
        line=dict(color="black", width=3),
        name="Ward Boundary",
        hoverinfo='skip',
        opacity=0.7
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


def calc_avg_distance_between_crime_and_officer(clustered_data, centroids):

    df = pd.merge(
        left=clustered_data, 
        right=pd.DataFrame(centroids, columns=["latitude_assigned_officer", "longitude_assigned_officer"]), 
        how="left", 
        left_on="cluster", 
        right_index=True
        )

    df["distance"] = df.apply(
        lambda row: geodesic(
            (row["latitude"], row["longitude"]),
            (row["latitude_assigned_officer"], row["longitude_assigned_officer"])
        ).meters,
        axis=1
    )

    return df["distance"].mean().round(decimals=3), df["distance"].max().round(decimals=3)


def calc_street_distance_between_crime_and_officer(clustered_data, centroids):
    # The algo



    pass
    # return mean_distance, max_distance