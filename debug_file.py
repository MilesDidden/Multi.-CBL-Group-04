import sqlite3

conn = sqlite3.connect("data/crime_data_UK_v4.db")
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("Tables in the database:")
for table in tables:
    print(table[0])

conn.close()

def load_ward_options(db_path: str = "data/crime_data_UK_v4.db"):
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT ward_code, ward_name FROM ward_location"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return [{"label": row["ward_name"], "value": row["ward_code"]} for _, row in df.iterrows()]
    except Exception as e:
        print(f"Error loading ward options: {e}")
        return []
