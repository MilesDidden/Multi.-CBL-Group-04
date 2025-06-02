from DB_utils import DBhandler


def create_temp_table(ward_code: str, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db")-> None:

    db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)

    # Create a temporary table for the selected ward
    create_temp_table_query = f"""
            CREATE TABLE IF NOT EXISTS temp_crime_{ward_code} AS
            SELECT * FROM crime
            WHERE ward_code = '{ward_code}';
        """
    db_handler.update(create_temp_table_query) 

    db_handler.close_connection_db()


def delete_temp_table(ward_code: str, db_loc: str="../data/", db_name: str="crime_data_UK_v4.db") -> None:
    
    db_handler = DBhandler(db_loc=db_loc, db_name=db_name, verbose=0)

    drop_query = f"DROP TABLE IF EXISTS temp_crime_{ward_code};"
    db_handler.update(drop_query)
    db_handler.close_connection_db()
