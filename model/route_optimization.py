import math
import openrouteservices as ors
from DB_utils import DBhandler
import random

ors_api = "5b3ce3597851110001cf6248e221bd2f17534d91b74e79bba6167299"


def VRP(crime_locations, officer_location, api: str=ors_api):
    ### Implementation ###
    #Simple single route optimization
    client = ors.Client(key='YOUR_KEY_HERE')
    # time_window = maximum number of hours a police officer can work on a day (Do we need it?)
    police_officers = [
        ors.optimization.officer(id=0, profile='agent', start=officer_location, end=officer_location, time_window=[0, 8*60*60]),
        ors.optimization.officer(id=1, profile='agent', start=officer_location, end=officer_location, time_window=[0, 8*60*60])
    ]
    
    jobs = [ors.optimization.Job(id=index, **job) for index, job in enumerate(crime_locations)]
    optimized = client.optimization(jobs=jobs, police_officers = police_officers, geometry=True)

    return optimized 


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
