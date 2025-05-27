from model.DB_utils import *
from shapely.wkt import dumps as wkt_dumps
from tqdm import tqdm


if __name__ == "__main__":

    # Establish connection
    db_handler = DBhandler(db_loc="../data", db_name="crime_data_UK_v3.db")

    # Create table force_districts
    db_handler.create_table(
        table_name='force_districts',
        columns={'force_district_name': 'TEXT PRIMARY KEY',
                    'multipolygon':'TEXT'
                    }
    )

    # Preprocess data for force_districts
    path_to_district_kml_files = "data/force_kmls/"
    list_of_district_kmls = os.listdir(path_to_district_kml_files)

    df_polygons_of_districts = []
    for district_kml in list_of_district_kmls:
        df_polygons_of_districts.append(parse_kml_multipolygon(parent_path=path_to_district_kml_files, kml_file=district_kml))

    df_districts = pd.concat(df_polygons_of_districts, ignore_index=True)
    df_districts.multipolygon = df_districts.multipolygon.apply(lambda x: json.dumps(x))

    # Insert data into force_districts
    db_handler.insert_rows(
        table_name='force_districts',
        data=df_districts.to_dict(orient='records')
    )

    # Create table crime data
    db_handler.create_table(
        table_name='crime',
        columns={'crime_id':'TEXT PRIMARY KEY',
                    'month':'TEXT',
                    'reported_by':'TEXT',
                    'falls_within':'TEXT',
                    'long':'REAL',
                    'lat':'REAL',
                    'location':'TEXT',
                    'lsoa_code':'TEXT',
                    'crime_type':'TEXT',
                    'last_outcome_category':'TEXT'
                    }
    )

    for file in tqdm(list_all_street_crime_csv_files()):
        temp_df = extract_and_transform_crime_data(file, True, db_handler.existing_crime_ids)

        if not temp_df.empty:

            temp_df = temp_df.drop(columns=["LSOA name", "Context"])

            temp_df.columns = temp_df.columns.str.strip().str.lower()

            temp_df = temp_df.rename(
                columns={
                    "crime id": "crime_id",
                    "month": "month",
                    "reported by": "reported_by",
                    "falls within": "falls_within",
                    "longitude": "long",
                    "latitude": "lat",
                    "location":"location",
                    "lsoa code": "lsoa_code",
                    "crime type":"crime_type",
                    "last outcome category":"last_outcome_category"
                }
            )
            # print(temp_df.head())
            
            db_handler.insert_rows(
                table_name='crime',
                data=temp_df.to_dict(orient='records')
            )
        
        else:
            print("Empty dataframe, skipping file ...")

    print("\nInserted all crime data with crime ids!\n")

    for file in tqdm(list_all_street_crime_csv_files()):
        temp_df = extract_and_transform_crime_data(file, False, db_handler.existing_crime_ids)

        if not temp_df.empty:

            temp_df = temp_df.drop(columns=["LSOA name", "Context"])

            temp_df.columns = temp_df.columns.str.strip().str.lower()

            temp_df = temp_df.rename(
                columns={
                    "crime id": "crime_id",
                    "month": "month",
                    "reported by": "reported_by",
                    "falls within": "falls_within",
                    "longitude": "long",
                    "latitude": "lat",
                    "location":"location",
                    "lsoa code": "lsoa_code",
                    "crime type":"crime_type",
                    "last outcome category":"last_outcome_category"
                }
            )
            
            db_handler.insert_rows(
                table_name='crime',
                data=temp_df.to_dict(orient='records')
            )

        else:
            print("Empty dataframe, skipping this file ...")

    print("\nInserted all data with generated crime ids!\n")

    db_handler.create_table("existing_crime_ids", columns={
        "existing_crime_id": "TEXT PRIMARY KEY"
    })

    db_handler.insert_rows("existing_crime_ids", data=[{"existing_crime_id":i} for i in db_handler.existing_crime_ids])

    # Extract & transform lsoa data
    lsoa_df = combine_all_lsoa_data_files(list_lsoa_data_files())

    lsoa_df = lsoa_df[["lsoa21cd", "lsoa21nm", "geometry"]].rename(columns={
        "lsoa21cd":"lsoa_code",
        "lsoa21nm":"lsoa_name"
    })

    lsoa_df["geometry"] = lsoa_df["geometry"].apply(wkt_dumps)

    # Create lsoa table
    db_handler.create_table("lsoa_location", columns={
        'lsoa_code':'TEXT PRIMARY KEY',
        'lsoa_name':'TEXT',
        'geometry':'TEXT'
    })

    # Insert LSOA data
    db_handler.insert_rows("lsoa_location", data=lsoa_df.to_dict(orient='records'))


    # Extract & transform ward data
    ward_df = gpd.read_file("data/Wards_December_2016_Boundaries_UK_BFE_2022_-5810284385438997272")
    ward_df = ward_df[["WD16CD", "WD16NM", "geometry"]].rename(columns={
        "WD16CD":"ward_code",
        "WD16NM":"ward_name"
    })
    ward_df["geometry"] = ward_df["geometry"].apply(wkt_dumps)


    # Create ward table
    db_handler.create_table("ward_location", columns={
        'ward_code':'TEXT PRIMARY KEY',
        'ward_name':'TEXT',
        'geometry':'TEXT'
    })

    # Insert ward data
    db_handler.insert_rows("ward_location", data=ward_df.to_dict(orient="records"))

    #Extract & transform IMD data

    db_handler.delete_table("imd_data")

    imd_df = pd.read_csv("data/imd2019lsoa.csv").reset_index()
    imd_df = imd_df[["FeatureCode", "Measurement", "Value", "Indices of Deprivation"]].rename(columns={
        "index":"uuid_imd",
        "FeatureCode":"feature_code",
        "Measurement":"measurement",
        "Value":"value",
        "Indices of Deprivation":"indices_of_deprivation"
    })

    # Create IMD table
    db_handler.create_table("imd_data", columns={
        'uuid_imd':'INTEGER PRIMARY KEY',
        'feature_code':'TEXT',
        'measurement':'TEXT',
        'value':'REAL',
        'indices_of_deprivation':'TEXT'
    })

    # Insert imd data
    db_handler.insert_rows("imd_data", data=imd_df.to_dict(orient="records"))


    # Close Connection
    db_handler.close_connection_db()
