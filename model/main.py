from DB_utils import DBhandler


if __name__ == "__main__":
    
    db_handler = DBhandler(db_name="crime_data_UK_v4.db")

    example_ward_code = 'E05000138'

    crime_data = db_handler.query(f"SELECT * FROM crime WHERE ward_code = '{example_ward_code}'")

    db_handler.close_connection_db()

    crime_data_grouped = crime_data.groupby(by="month").agg(crime_rate=("crime_id", "count"), mean_imd_value=("average_imd_decile", "mean"), covid_indicator=("covid_indicator", "first"), stringency_index=("stringency_index", "first"))

    print(crime_data_grouped)
