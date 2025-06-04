import math
import openrouteservice as ors
from DB_utils import DBhandler
import random

ors_api = "5b3ce3597851110001cf6248e221bd2f17534d91b74e79bba6167299"


def VRP(crime_locations, officer_location, api: str=ors_api):
    ### Implementation ###
    #Simple single route optimization
    client = ors.Client(key=ors_api)
    jobs = []
    for i, (lat, lon) in enumerate(crime_locations):
        jobs.append({
            "id": i,
            "location": [lon, lat]  # ORS usa [lon, lat]
        })
    
    vehicles = []
    for i, (lat, lon) in enumerate(officer_locations):
        vehicles.append({
            "id": i,
            "start": [lon, lat],
            "end": [lon, lat],
            "time_window": [0, 8 * 60 * 60],  # jornada de 8 horas
            "profile": "driving-car"
        })
    
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
            700;
        """ 
    ).to_numpy().tolist()
    db_handler.close_connection_db()

    example_police_officers_locations = [
        [random.uniform(51.510, 51.525), random.uniform(-0.129,-0.105)] for _ in range(100)
        ] # These are random examples of police officer locations

    result = VRP(crimes, example_police_officers_locations)
