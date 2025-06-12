from model.DB_utils import DBhandler
import plotly.graph_objects as go
from shapely import wkt
import geopandas as gpd
import numpy as np
from sklearn.metrics import pairwise_distances_argmin
import pandas as pd
from geopy.distance import geodesic
from tqdm import tqdm
import networkx as nx
import osmnx as ox


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
    initial_idxs = rng.choice(coords.shape[0], n_clusters, replace=True)
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


def plot_kmeans_clusters(clustered_data, centroids, ward_code, ward_name, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db"):

    fig = go.Figure()

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

    # Prepare centroid hover text
    centroid_hover_texts = [
        f"Police Officer {i}<br>Lat: {lat:.5f}<br>Lon: {lon:.5f}"
        # stays the same, just ensure the unpacking is correct
        for i, (lat, lon) in enumerate(centroids)
    ]

    fig.add_trace(go.Scattermapbox(
        lat=centroids[:, 0],
        lon=centroids[:, 1],
        mode="markers",
        marker=dict(
            size=8,
            color="blue",
            opacity=0.85,
        ),
        name="Police Officers",
        hoverinfo="text",
        hovertext=centroid_hover_texts
    ))

    # Crime cluster points
    fig.add_trace(go.Scattermapbox(
    lat=clustered_data["latitude"],
    lon=clustered_data["longitude"],
    mode="markers",
    marker=dict(
        size=5,
        color="red",
        opacity=0.85,
    ),
    name="Crime Clusters",
    text=[f"Crime {c}" for c in clustered_data["cluster"]],
    hoverinfo="text"
    ))

    # Compute center of ward polygon
    center = geom.centroid
    center_lat = center.y
    center_lon = center.x

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            zoom=12,  # Your desired zoom level
            center=dict(lat=center_lat, lon=center_lon)
        ),
        title=f"K-Means Clustering of Crimes in Ward {ward_name}",
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

    return round(df["distance"].mean(), 3), round(df["distance"].max(), 3)


def calc_street_distance_between_crime_and_officer(clustered_data, centroids, graph=None):
    """
    Calculate the street distance (shortest path in meters) between each crime location and its assigned officer.

    Arguments:
    - clustered_data: DataFrame with columns ["latitude", "longitude", "cluster"]
    - centroids: numpy array of shape (n_clusters, 2), with [latitude, longitude] for each officer
    - graph: preloaded OSMnx graph (optional). If None, will load from file.

    Returns:
    - mean_distance (meters), max_distance (meters)
    """

    # Load graph if needed
    if graph is None:
        print(f"No graph is given for the calculation of street distance, extracting internally...")
        G = ox.load_graphml("data/london_map_drive.graphml")
    else:
        G = graph

    # Precompute edge lengths
    G = ox.distance.add_edge_lengths(G)

    distances = []

    for idx, row in tqdm(clustered_data.iterrows(), total=len(clustered_data)):
        # Crime point (latitude, longitude)
        crime_point = (row["latitude"], row["longitude"])

        # Officer (centroid) point for this crime's cluster
        cluster_idx = int(row["cluster"])  # Convert to integer index (IMPORTANT FIX)
        officer_point = (centroids[cluster_idx][0], centroids[cluster_idx][1])

        try:
            # Find nearest nodes in graph
            crime_node = ox.distance.nearest_nodes(G, X=crime_point[1], Y=crime_point[0])
            officer_node = ox.distance.nearest_nodes(G, X=officer_point[1], Y=officer_point[0])

            # Calculate shortest path length (meters)
            length = nx.shortest_path_length(G, crime_node, officer_node, weight="length")

        except Exception as e:
            print(f"Warning: could not compute path for row {idx}: {e}")
            length = float("nan")

        distances.append(length)

    # Add distances to dataframe (optional — if you want to inspect later)
    clustered_data["street_distance_meters"] = distances

    # Compute statistics (ignoring NaNs)
    # Compute statistics (ignoring NaNs)
    mean_distance = round(clustered_data["street_distance_meters"].mean(), 3)
    max_distance = round(clustered_data["street_distance_meters"].max(), 3)


    return mean_distance, max_distance
