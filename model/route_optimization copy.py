import openrouteservice as ors
from DB_utils import DBhandler
import random
import plotly.graph_objects as go
import polyline


ors_api = "5b3ce3597851110001cf6248e221bd2f17534d91b74e79bba6167299"


def VRP(crime_locations, officer_location, api: str=ors_api):
    ### Implementation ###
    #Simple single route optimization
    client = ors.Client(key=api)
    jobs = []
    for i, (lat, lon) in enumerate(crime_locations):

        jobs.append(
            ors.optimization.Job(id=i, location=[lon, lat], amount=[1])
        )

        # jobs.append({
        #     "id": i,
        #     "location": [lon, lat]  # ORS usa [lon, lat]
        # })
    
    vehicles = []
    for j, (lat, lon) in enumerate(officer_location):
        vehicles.append(
            ors.optimization.Vehicle(id=j, profile='driving-car', start=[lon, lat], end=[lon, lat], capacity=[20])
        )




        # vehicles.append({
        #     "id": i,
        #     "start": [lon, lat],
        #     "end": [lon, lat],
        #     "time_window": [0, 8 * 60 * 60],  # jornada de 8 horas
        #     "profile": "driving-car"
        # })
    
    optimized = client.optimization(jobs=jobs, vehicles=vehicles, geometry=True)
    return optimized
    
    
    
    
    #for location in officer_location:
    #    location = client.optimization(crime_locations=crime_locations) 
    #for crime in crime_locations:
    #    crime = client.optimization(crime_locations=crime_locations)
    #optimized = client.optimization(crime_locations=crime_locations, officer_location = officer_location, geometry=True)
    
    #jobs = [ors.optimization.Job(id=index, **job) for index, job in enumerate(crime_locations)]
    #optimized = client.optimization(jobs=jobs, officer_location = officer_location, geometry=True)

    #return optimized 


def decode_polyline(polyline_str):
    # Decode the polyline string using the 'polyline' library
    return polyline.decode(polyline_str)


def plot_vrp(crime_locations, officer_location, optimized):
    # Extract the geometry (encoded polyline string)
    geometry_str = optimized['routes'][0]['geometry']
    
    # Decode the polyline string into coordinates
    geometry = decode_polyline(geometry_str)
    
    # Convert the geometry path into latitude and longitude
    path_latitudes = [coord[0] for coord in geometry]
    path_longitudes = [coord[1] for coord in geometry]
    
    # Extract crime and officer locations
    crime_latitudes = [lat for lat, lon in crime_locations]
    crime_longitudes = [lon for lat, lon in crime_locations]
    officer_latitudes = [lat for lat, lon in officer_location]
    officer_longitudes = [lon for lat, lon in officer_location]
    
    # Create the plot
    fig = go.Figure()
    
    # Plot the crime locations
    fig.add_trace(go.Scattergeo(
        lat=crime_latitudes,
        lon=crime_longitudes,
        mode='markers',
        name='Crimes',
        marker=dict(color='red', size=10, symbol='x')
    ))
    
    # Plot the police officer locations
    fig.add_trace(go.Scattergeo(
        lat=officer_latitudes,
        lon=officer_longitudes,
        mode='markers',
        name='Police Officers',
        marker=dict(color='blue', size=10, symbol='circle')
    ))
    
    # Plot the optimized path
    for route in optimized['routes']:
        try:
            geometry_str = route['geometry']
            # Decode the polyline string into coordinates
            geometry = decode_polyline(geometry_str)

            # Convert the geometry path into latitude and longitude (reverse coordinates if needed)
            path_latitudes = [coord[0] for coord in geometry]  # Reverse order for proper lat/lon
            path_longitudes = [coord[1] for coord in geometry]  # Reverse order for proper lat/lon

            # Plot the optimized path for each route
            fig.add_trace(go.Scattergeo(
                lat=path_latitudes,
                lon=path_longitudes,
                mode='lines',
                name=f'Optimized Path {route["vehicle"]}',
                line=dict(width=2)
            ))
        except KeyError:
            print("Error: Geometry data not found for a route.")
            continue
    
    # Set the layout for the map
    fig.update_layout(
        mapbox=dict(
            style="stamen-terrain",  # Try 'stamen-terrain' or 'stamen-toner' for detailed street-level maps
            zoom=11,  # Adjust zoom level
            center=dict(lat=51.5074, lon=-0.1278)  # Center on London (adjust as needed)
        ),
        title="Crime Locations, Police Officers, and Optimized Paths",
        showlegend=True,
        autosize=True,
        height=800,
        margin=dict(l=0, r=0, t=50, b=0)
    )
    
    fig.write_html("temp_route_optimization.html", auto_open=True)


if __name__ == "__main__":

    # Can ignore the code below, assume its correct. If not, lmk.
    db_handler = DBhandler("../data/", "crime_data_UK_v4.db")
    crimes = db_handler.query( # These represent 700 crimes randomly sampled from the historical data
        """
        SELECT 
            lat AS latitude, 
            long AS longitude
        FROM 
            crime
        WHERE 
            lat IS NOT NULL 
            AND long IS NOT NULL
            AND ward_code = 'E05000138'
        ORDER BY 
            RANDOM()
        LIMIT 
            50;
        """ 
    ).to_numpy().tolist()
    db_handler.close_connection_db()

    example_police_officers_locations = [
        (random.uniform(51.510, 51.525), random.uniform(-0.129,-0.105)) for _ in range(3)
        ] # These are random examples of police officer locations

    path_optimized = VRP(crimes, example_police_officers_locations)

    # Plot result
    plot_vrp(crimes, example_police_officers_locations, path_optimized)

