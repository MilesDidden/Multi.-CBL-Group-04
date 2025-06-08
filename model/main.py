from ML_utils import create_temp_table, delete_temp_table
from SARIMAX import timeseries
from KMeans import run_kmeans_weighted, plot_kmeans_clusters, calc_avg_distance_between_crime_and_officer


db_loc = "../data/"
db_name = "crime_data_UK_v4.db"


if __name__ == "__main__":
    
    # Run dashboard (which waits for inputs)
    ######## ??? #######

    # temporary output --> delete later ???:
    ward_code = "E05000138"
    num_police_officers = 100

    # Create temporary table to work with for ML models
    create_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

    # Run timeseries 
    timeseries_figure, number_of_predicted_crimes, weight_imd = timeseries(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

    # Run KMeans
    centroids, clustered_data = run_kmeans_weighted(ward_code=ward_code, n_crimes=number_of_predicted_crimes, imd_value=weight_imd, n_clusters=num_police_officers, db_loc=db_loc, db_name=db_name)
    print(f"Police officers allocation for {ward_code}:\n", centroids)

    # Plot KMeans results
    fig = plot_kmeans_clusters(clustered_data=clustered_data, centroids=centroids, ward_code=ward_code)
    fig.write_html("kmeans_clusters_plot.html", auto_open=True)

    # Calculate distances of officers to crimes
    mean_dist, max_dist = calc_avg_distance_between_crime_and_officer(clustered_data, centroids)
    print(f"\nAverage euclidean distance of a police officer to a crime: {mean_dist} [m]")
    print(f"Maximum euclidean distance of a police officer to a crime: {max_dist} [m]\n")

    # Delete temp table
    delete_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

    # Return results to dashboard & show it
    ######### ??? ######
