from DB_utils import *
from shapely.wkt import dumps as wkt_dumps


if __name__ == "__main__":

    # Establish connection
    db_handler = DBhandler(db_loc="data/", db_name="crime_data_UK.db")

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

    for file in list_all_street_crime_csv_files():
        temp_df = extract_and_transform_crime_data(file, True, db_handler.existing_crime_ids).drop(columns=["LSOA name", "Context"])

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

    print("\nInserted all crime data with crime ids!\n")

    for file in list_all_street_crime_csv_files():
        temp_df = extract_and_transform_crime_data(file, False, db_handler.existing_crime_ids).drop(columns=["LSOA name", "Context"])

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


    # Close Connection
    db_handler.close_connection_db()
