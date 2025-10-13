import glob
import logging
import os
import shutil
import time
from functools import partial
from getpass import getpass
from multiprocessing import Pool, cpu_count

import geopandas as gpd
import numpy as np
import psycopg2
import rasterio
from psycopg2.extras import execute_batch
from rasterio.mask import mask
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)


def insert_data_to_db(data, conn, chunk_size=100000):
    """
    Insert data into the PostgreSQL database in chunks.
    """
    cursor = conn.cursor()

    for i in range(0, len(data), chunk_size):
        data_chunk = data[i : i + chunk_size]
        query = """
        INSERT INTO geospatial_data_worldpop_age_sex (gid, admin_level, date, variable, sum, mean, min, max, missing_value_percentage, source, unit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (gid, admin_level, date, variable) 
        DO UPDATE SET sum = EXCLUDED.sum, mean = EXCLUDED.mean, min = EXCLUDED.min, max = EXCLUDED.max, 
                      missing_value_percentage = EXCLUDED.missing_value_percentage, source = EXCLUDED.source, unit = EXCLUDED.unit
        """
        execute_batch(cursor, query, data_chunk)
        conn.commit()
        print(f"Inserted chunk of {len(data_chunk)} rows.")

    conn.commit()


def calculate_cell_area(src):
    """
    Calculate the area of each cell in the raster.
    """
    res_x, res_y = src.res
    area_sq_degrees = np.abs(res_x * res_y)
    earth_radius = 6371000  # meters
    lat_radians = np.deg2rad(
        src.xy(np.arange(src.height), np.zeros(src.height))[1]
    )
    area = (
        area_sq_degrees
        * (np.pi / 180) ** 2
        * earth_radius**2
        * np.cos(lat_radians)
    )

    return area[:, np.newaxis] * np.ones(
        src.width
    )  # Broadcasting instead of np.tile


def calculate_stats(src, geometry, cell_area):
    """
    Calculate statistics for the given data and geometry.
    """
    try:
        masked_data, masked_transform = mask(
            src, [geometry], crop=True, all_touched=True, nodata=src.nodata
        )
        masked_data = masked_data[0]  # Get the first band

        valid_mask = masked_data != src.nodata

        if not np.any(valid_mask):
            return None, None, None, None, 100.0

        data_sum = np.sum(masked_data[valid_mask])
        data_min = np.min(masked_data[valid_mask])
        data_max = np.max(masked_data[valid_mask])

        window = src.window(*geometry.bounds)
        window_start_row = int(window.row_off)
        window_end_row = min(int(window.row_off + window.height), src.height)
        window_start_col = int(window.col_off)
        window_end_col = min(int(window.col_off + window.width), src.width)

        cell_area_slice = cell_area[
            window_start_row:window_end_row, window_start_col:window_end_col
        ]

        min_height = min(cell_area_slice.shape[0], valid_mask.shape[0])
        min_width = min(cell_area_slice.shape[1], valid_mask.shape[1])
        cell_area_slice = cell_area_slice[:min_height, :min_width]
        valid_mask = valid_mask[:min_height, :min_width]

        weights = cell_area_slice[valid_mask]
        weighted_sum = np.sum(
            masked_data[:min_height, :min_width][valid_mask] * weights
        )
        weight_sum = np.sum(weights)
        data_mean = weighted_sum / weight_sum if weight_sum > 0 else None

        total_cells = valid_mask.size
        valid_cells = np.sum(valid_mask)
        missing_value_percentage = (
            (1 - valid_cells / total_cells) * 100 if total_cells > 0 else 100.0
        )

        return (
            data_sum,
            data_mean,
            data_min,
            data_max,
            missing_value_percentage,
        )
    except ValueError:
        return None, None, None, None, 100.0


def process_chunk(chunk, geotiff_path, variable_name, level, date):
    """
    Process a chunk of geometries.
    """
    results = []
    gid_column = f"GID_{level}"

    with rasterio.open(geotiff_path) as src:
        cell_area = calculate_cell_area(src)
        unit = src.tags().get("units", "unknown")
        # raster_data = src.read(1)
        for _, row in chunk.iterrows():
            geometry = row["geometry"]
            gid = row[gid_column]

            (
                data_sum,
                data_mean,
                data_min,
                data_max,
                missing_value_percentage,
            ) = calculate_stats(src, geometry, cell_area)

            results.append(
                (
                    gid,
                    level,
                    date,
                    variable_name,
                    float(data_sum) if data_sum is not None else None,
                    float(data_mean) if data_mean is not None else None,
                    float(data_min) if data_min is not None else None,
                    float(data_max) if data_max is not None else None,
                    round(float(missing_value_percentage), 2),
                    "WorldPop",
                    unit,
                )
            )

    return results


def find_files(geotiff_folder, variable_name):
    """
    Find all relevant files for a given variable, including those in processed folders.

    :param geotiff_folder: Base directory for GeoTIFF files
    :param variable_name: Name of the variable to process
    :return: List of file paths
    """
    file_pattern = "*.tif"

    # Find files in the main directory
    main_files = glob.glob(os.path.join(geotiff_folder, file_pattern))

    # Find files in processed folders
    processed_files = []
    for level in range(2):  # Assuming we have levels 0 and 1
        level_folder = os.path.join(
            geotiff_folder, "processed", f"level_{level}"
        )
        if os.path.exists(level_folder):
            processed_files.extend(
                glob.glob(os.path.join(level_folder, file_pattern))
            )

    # Combine and sort all found files
    all_files = sorted(set(main_files + processed_files))
    return all_files


def get_processed_level(file_path, geotiff_folder):
    """
    Determine the highest level at which a file has been processed.

    :param file_path: Path to the file
    :param geotiff_folder: Base directory for GeoTIFF files
    :return: Highest processed level, or -1 if not processed
    """
    if "processed" not in file_path:
        return -1

    # Extract level from the file path
    parts = file_path.split(os.sep)
    level_index = parts.index("processed") + 1
    if level_index < len(parts):
        return int(parts[level_index].split("_")[1])

    return -1


def move_processed_file(file_path, geotiff_folder, level):
    """
    Move the processed file to a nested folder structure reflecting the processed level.

    :param file_path: Path to the file to be moved
    :param geotiff_folder: Base directory for GeoTIFF files
    :param level: Administrative level that was processed
    """
    processed_folder = os.path.join(
        geotiff_folder, "processed", f"level_{level}"
    )
    os.makedirs(processed_folder, exist_ok=True)

    new_file_path = os.path.join(processed_folder, os.path.basename(file_path))
    shutil.move(file_path, new_file_path)

    print(f"Moved processed file {file_path} to {new_file_path}")


def process_level(
    geopackage_path, level, geotiff_path, variable_name, db_conn, date
):
    """
    Process a specific administrative level to calculate statistics and insert data into the database.
    """
    print(f"Processing level {level}")

    gdf = gpd.read_file(geopackage_path, layer=f"ADM_{level}")

    with rasterio.open(geotiff_path) as src:
        gdf = gdf.to_crs(src.crs)

    gid_column = f"GID_{level}"
    gdf = gdf[[gid_column, "geometry"]]

    total_features = len(gdf)
    print(f"total_areal_features (ADM_{level}): {total_features}")

    # Split the dataframe into chunks
    num_processes = min(
        cpu_count(), 2
    )  # Use 2 processes or less if CPU count is lower
    chunk_size = max(
        1, total_features // (num_processes * 4)
    )  # Adjust chunk size based on number of processes
    chunks = [
        gdf.iloc[i : i + chunk_size]
        for i in range(0, total_features, chunk_size)
    ]

    # Process chunks in parallel
    with Pool(processes=num_processes) as pool:
        process_chunk_partial = partial(
            process_chunk,
            geotiff_path=geotiff_path,
            variable_name=variable_name,
            level=level,
            date=date,
        )
        all_results = []
        for chunk_result in tqdm(
            pool.imap(process_chunk_partial, chunks),
            total=len(chunks),
            desc=f"Processing {variable_name} - level {level}",
        ):
            all_results.extend(chunk_result)

            # Insert data in smaller batches to manage memory
            if len(all_results) >= 10000:
                insert_data_to_db(all_results, db_conn)
                all_results = []

    # Insert any remaining results
    if all_results:
        insert_data_to_db(all_results, db_conn)

    print(f"\nProcessing complete for level {level}")
    return total_features


def main():
    """
    Main function to process multiple GeoTIFF files from a folder and insert data into the PostgreSQL database.
    Uses the base folder name as the date year.
    """
    # Get the folder path containing GeoTIFF files
    geotiff_folder = input(
        "Enter the path to the folder containing GeoTIFF files: "
    )
    geopackage_file_path = input("Enter the path to the GeoPackage file: ")

    # Extract the year from the base folder name
    base_folder_name = os.path.basename(os.path.normpath(geotiff_folder))
    try:
        year = int(base_folder_name)
        date = f"{year}-01-01"
    except ValueError:
        # Raise an error if the year cannot be extracted from the folder name
        raise ValueError(
            f"Could not extract year from folder name '{base_folder_name}'. Please ensure the folder name is a valid year."
        )

    print(f"Using date: {date}")

    db_params = {
        "dbname": "merge",
        "user": "postgres",
        "password": getpass("Enter the database password: "),
        "host": input("Enter the database host: "),
        "port": "5432",
    }

    conn = psycopg2.connect(**db_params)

    levels = [0, 1]

    try:
        for level in levels:
            print(f"\nProcessing all files for level {level}")
            # Find all .tif files in the folder and processed subfolders
            all_files = find_files(geotiff_folder, "")
            for file_path in all_files:
                filename = os.path.basename(file_path)

                # Extract variable name from the filename
                file_parts = filename.split("_")
                if len(file_parts) == 5:
                    variable_name = f"population_sex_age_{file_parts[1]}_{file_parts[2]}_count"
                elif len(file_parts) >= 6:
                    variable_name = f"2021_2022_population_sex_age_{file_parts[1]}_{file_parts[2]}_{file_parts[3]}_count"
                else:
                    print(
                        f"Skipping file {filename} due to unexpected naming format"
                    )
                    continue

                print(f"\nProcessing file: {filename}")
                print(f"Variable name: {variable_name}")

                processed_level = get_processed_level(
                    file_path, geotiff_folder
                )
                if processed_level >= level:
                    print(
                        f"Skipping {file_path} as it has already been processed at level {processed_level}"
                    )
                    continue

                start_time = time.time()

                processed_rows = process_level(
                    geopackage_file_path,
                    level,
                    file_path,
                    variable_name,
                    conn,
                    date,
                )

                end_time = time.time()

                print(f"\nLevel {level} Results:")
                print(
                    f"Processed {processed_rows} rows in {end_time - start_time:.2f} seconds"
                )

                # Move the processed file to the nested folder structure
                move_processed_file(file_path, geotiff_folder, level)

            print(f"Completed processing all files for level {level}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
