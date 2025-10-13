import json
import os
from getpass import getpass

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


def process_geojson_for_db(file_path):
    """
    Process GeoJSON file and prepare data for database insertion.

    :param file_path: Path to the GeoJSON file
    :return: List of dictionaries containing the processed data
    """
    # Read the GeoJSON file
    gdf = gpd.read_file(file_path)

    # Convert year to date (assuming first day of the year)
    gdf["date"] = pd.to_datetime(gdf["year"].astype(str) + "-01-01")

    # Prepare the data for database insertion
    db_data = []
    for _, row in gdf.iterrows():
        metadata = {
            "Lon": row["lon"],
            "Lat": row["lat"],
            "ISO": row["ISO"],
            "Name": row["Name"],
            "PWC_Lat": row["PWC_Lat"],
            "PWC_Lon": row["PWC_Lon"],
            "Pop": row["Pop"],
            "Density": row["Density"],
            "Area": row["Area"],
        }

        db_row = {
            "gid": row["ISO"],
            "admin_level": 0,
            "date": row["date"].strftime("%Y-%m-%d"),
            "variable": "Population_Weighted_Density_G",
            "sum": None,
            "mean": None,
            "min": None,
            "max": None,
            "raw_value": row["PWD_G"],
            "note": "GID directly from WorldPop",
            "source": "WorldPop",
            "metadata": json.dumps(metadata),
        }
        db_data.append(db_row)

    return db_data


def insert_data_to_db(data, conn, chunk_size=100000):
    """
    Insert data into the PostgreSQL database in chunks.

    :param data: List of dictionaries containing the data to be inserted
    :param conn: Database connection object
    :param chunk_size: Number of rows to insert in each batch
    """
    cursor = conn.cursor()

    for i in range(0, len(data), chunk_size):
        data_chunk = data[i : i + chunk_size]
        query = """
        INSERT INTO geospatial_data_worldpop_pwd (gid, admin_level, date, variable, sum, mean, min, max, raw_value, note, source, metadata)
        VALUES (%(gid)s, %(admin_level)s, %(date)s, %(variable)s, %(sum)s, %(mean)s, %(min)s, %(max)s, %(raw_value)s, %(note)s, %(source)s, %(metadata)s)
        ON CONFLICT (gid, admin_level, date, variable) 
        DO UPDATE SET sum = EXCLUDED.sum, mean = EXCLUDED.mean, min = EXCLUDED.min, max = EXCLUDED.max, 
                      raw_value = EXCLUDED.raw_value, note = EXCLUDED.note, source = EXCLUDED.source, metadata = EXCLUDED.metadata
        """
        execute_batch(cursor, query, data_chunk)
        conn.commit()
        print(f"Inserted chunk of {len(data_chunk)} rows.")

    conn.commit()


def main(folder_path, db_params):
    """
    Main function to process all GeoJSON files within a folder and insert data into the database.

    :param folder_path: Path to the folder containing GeoJSON files
    :param db_params: Dictionary containing database connection parameters
    """
    # Initialize an empty list to hold all processed data
    all_db_data = []

    # Loop through all files in the folder, ignoring hidden files
    for file in os.listdir(folder_path):
        if file.endswith(".geojson") and not file.startswith("."):
            file_path = os.path.join(folder_path, file)
            db_data = process_geojson_for_db(file_path)
            all_db_data.extend(db_data)

    conn = psycopg2.connect(**db_params)
    insert_data_to_db(all_db_data, conn)
    conn.close()

    print(f"Inserted {len(all_db_data)} rows into the database.")


if __name__ == "__main__":
    folder_path = input(
        "Enter the path to the folder containing GeoJSON files: "
    )
    db_params = {
        "dbname": "merge",
        "user": "postgres",
        "password": getpass("Enter the database password: "),
        "host": input("Enter the database host: "),
        "port": "5432",
    }
    main(folder_path, db_params)
