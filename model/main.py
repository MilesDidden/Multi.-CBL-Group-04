from ML_utils import create_temp_table, delete_temp_table
from SARIMAX import timeseries
from KMeans import run_kmeans, plot_kmeans_clusters


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
    timeseries_figure, number_of_predicted_crimes = timeseries(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

    # Run KMeans
    centroids, clustered_data = run_kmeans(ward_code=ward_code, n_crimes=number_of_predicted_crimes, n_clusters=num_police_officers, db_loc=db_loc, db_name=db_name)
    print(f"Police officers allocation for {ward_code}:\n", centroids)

    # Plot KMeans results
    fig = plot_kmeans_clusters(clustered_data=clustered_data, centroids=centroids, ward_code=ward_code)
    fig.write_html("kmeans_clusters_plot.html", auto_open=True)

    # Delete temp table
    delete_temp_table(ward_code=ward_code, db_loc=db_loc, db_name=db_name)

    # Return results to dashboard & show it
    ######### ??? ######