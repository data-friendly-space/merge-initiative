import gc
import glob
import json
import logging
import os
import shutil
import time
from collections import OrderedDict
from functools import partial
from getpass import getpass
from multiprocessing import Pool

import dask
import geopandas as gpd
import numpy as np
import psycopg2
import rioxarray
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from psycopg2.extras import execute_batch
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO)


def insert_data_to_db(data, conn, chunk_size=10000):
    """
    Insert data into the PostgreSQL database in chunks.

    :param data: List of tuples containing the data to be inserted
    :param conn: Database connection object
    :param chunk_size: Number of rows to insert in each batch
    """
    cursor = conn.cursor()

    for i in range(0, len(data), chunk_size):
        data_chunk = data[i : i + chunk_size]

        # Convert numpy types to Python types
        converted_data_chunk = []
        for row in data_chunk:
            converted_row = [
                float(val)
                if isinstance(val, np.floating)
                else int(val)
                if isinstance(val, np.integer)
                else val
                for val in row
            ]
            converted_data_chunk.append(tuple(converted_row))

        query = """
        INSERT INTO geospatial_data_landcover 
        (gid, admin_level, date, variable, sum, mean, min, max, raw_value, missing_value_percentage, note, source, unit, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (gid, admin_level, date, variable) 
        DO UPDATE SET 
        sum = EXCLUDED.sum, 
        mean = EXCLUDED.mean, 
        min = EXCLUDED.min, 
        max = EXCLUDED.max, 
        raw_value = EXCLUDED.raw_value, 
        missing_value_percentage = EXCLUDED.missing_value_percentage, 
        note = EXCLUDED.note, 
        source = EXCLUDED.source, 
        unit = EXCLUDED.unit, 
        metadata = EXCLUDED.metadata
        """
        execute_batch(cursor, query, converted_data_chunk)
        conn.commit()
        print(f"Inserted chunk of {len(converted_data_chunk)} rows.")

    conn.commit()


def get_flag_meanings_dict(da):
    flag_meanings = da.attrs["flag_meanings"].split()
    flag_values = da.attrs["flag_values"]
    return dict(zip(flag_values, flag_meanings))


def calculate_land_cover_stats(da, geometry):
    """
    Calculate land cover statistics for the given DataArray and geometry.

    :param da: xarray DataArray
    :param geometry: Shapely geometry object
    :return: Dictionary with land class counts and percentages, and missing value percentage
    """
    try:
        # Get the bounds of the geometry
        minx, miny, maxx, maxy = geometry.bounds

        # Add a small buffer to ensure we capture all relevant data
        buffer = max(da.rio.resolution()) * 2  # 2 pixels buffer
        minx, miny = minx - buffer, miny - buffer
        maxx, maxy = maxx + buffer, maxy + buffer

        # Clip the DataArray to the buffered bounds of the geometry
        da_clipped = da.sel(lon=slice(minx, maxx), lat=slice(maxy, miny))

        # Check if there's any data in the clipped region
        if da_clipped.isnull().all():
            print(f"No data found in the geometry bounds: {geometry.bounds}")
            return {}, 100.0

        # Perform the precise clipping operation
        clipped = da_clipped.rio.clip([geometry], all_touched=True)

        if clipped.isnull().all():
            print(
                f"Clipping resulted in no data for geometry with bounds: {geometry.bounds}"
            )
            return {}, 100.0

        values, counts = np.unique(
            clipped.values[~np.isnan(clipped.values)], return_counts=True
        )
        total_pixels = counts.sum()
        missing_pixels = np.isnan(clipped.values).sum()
        total_area = clipped.size
        missing_value_percentage = (missing_pixels / total_area) * 100

        flag_meanings = get_flag_meanings_dict(da)

        result = OrderedDict()
        for value, count in zip(values, counts):
            land_class = int(value)
            class_name = flag_meanings.get(land_class, "Unknown")
            percentage = float(count / total_pixels * 100)
            result[f"{land_class}_{class_name}"] = {
                "count": int(count),
                "percentage": percentage,
            }

        # Sort the result dictionary by percentage in descending order
        sorted_result = OrderedDict(
            sorted(
                result.items(), key=lambda x: x[1]["percentage"], reverse=True
            )
        )

        return sorted_result, missing_value_percentage
    except rioxarray.exceptions.NoDataInBounds:
        return {}, 100.0


def process_geometry(da, geometry, level, gid, date):
    """
    Process a single geometry to calculate land cover statistics.

    :param da: xarray DataArray with land cover data
    :param geometry: Shapely geometry object
    :param level: Administrative level
    :param gid: Geometry ID
    :param date: Date of the data
    :return: List of tuples with processed data
    """
    stats, missing_value_percentage = calculate_land_cover_stats(da, geometry)
    metadata = json.dumps(stats)

    results = []
    note = "sum is the count of the class, raw_value is the percentage of the class in that area"
    source = "Copernicus_CDS_LandClass"
    unit = "land class count or percentage per area"

    for class_name, class_stats in stats.items():
        results.append(
            (
                gid,
                level,
                date,
                class_name,
                int(class_stats["count"]),
                None,  # mean
                None,  # min
                None,  # max
                float(class_stats["percentage"]),
                float(missing_value_percentage),
                note,
                source,
                unit,
                metadata,
            )
        )

    return results


def process_batch(batch, da, level, date):
    """
    Process a batch of geometries to calculate land cover statistics.

    :param batch: GeoDataFrame with geometries
    :param da: xarray DataArray with land cover data
    :param level: Administrative level
    :param date: Date of the data
    :return: List of tuples with processed data
    """
    results = []
    gid_column = f"GID_{level}"
    with tqdm(
        total=len(batch), desc=f"Processing batch - level {level}", leave=False
    ) as pbar:
        for _, row in batch.iterrows():
            result = process_geometry(
                da, row["geometry"], level, row[gid_column], date
            )
            results.extend(result)
            pbar.update(1)
    return results


def find_files(data_directory, variable_name):
    """
    Find all relevant files for a given variable, including those in processed folders.

    :param data_directory: Base directory for data files
    :param variable_name: Name of the variable to process
    :return: List of file paths
    """
    file_pattern = f"*{variable_name.split('_')[0].upper()}*.nc"

    # Find files in the main directory
    main_files = glob.glob(os.path.join(data_directory, file_pattern))

    # Find files in processed folders
    processed_files = []
    for level in range(3):  # Assuming we have levels 0, 1, and 2
        level_folder = os.path.join(
            data_directory, "processed", f"level_{level}"
        )
        if os.path.exists(level_folder):
            processed_files.extend(
                glob.glob(os.path.join(level_folder, file_pattern))
            )

    # Combine and sort all found files
    all_files = sorted(set(main_files + processed_files))
    return all_files


def move_processed_file(file_path, data_directory, level):
    """
    Move the processed file to a nested folder structure reflecting the processed level.

    :param file_path: Path to the file to be moved
    :param data_directory: Base directory for data files
    :param level: Administrative level that was processed
    """
    processed_folder = os.path.join(
        data_directory, "processed", f"level_{level}"
    )
    os.makedirs(processed_folder, exist_ok=True)

    new_file_path = os.path.join(processed_folder, os.path.basename(file_path))
    shutil.move(file_path, new_file_path)

    print(f"Moved processed file {file_path} to {new_file_path}")


def get_processed_level(file_path, data_directory):
    """
    Determine the highest level at which a file has been processed.

    :param file_path: Path to the file
    :param data_directory: Base directory for data files
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


def process_level(
    geopackage_path,
    level,
    data_directory,
    variable_name,
    db_conn,
    use_dask=False,
):
    """
    Process a specific administrative level to calculate land cover statistics and insert data into the database.

    :param geopackage_path: Path to the GeoPackage file
    :param level: Administrative level to process
    :param data_directory: Base directory for data files
    :param variable_name: Name of the variable to process
    :param db_conn: Database connection object
    :param use_dask: Boolean flag to use Dask for processing
    :return: Number of processed rows
    """
    print(f"Processing level {level}")
    all_results_len = 0

    with Pool(processes=6) as pool:
        matching_files = find_files(data_directory, variable_name)

        if not matching_files:
            print(f"No file found for {variable_name}")
            return all_results_len

        try:
            for file_path in matching_files:
                processed_level = get_processed_level(
                    file_path, data_directory
                )
                if processed_level >= level:
                    print(
                        f"Skipping {file_path} as it has already been processed at level {processed_level}"
                    )
                    continue

                print(f"Processing {variable_name} from file: {file_path}")

                start_time = time.time()

                if use_dask:
                    with dask.config.set(
                        **{"array.slicing.split_large_chunks": True}
                    ):
                        ds = xr.open_dataset(
                            file_path,
                            chunks={"time": 1, "lat": 500, "lon": 500},
                        )
                        da = ds[variable_name].isel(time=0)
                else:
                    ds = xr.open_dataset(file_path)
                    da = ds[variable_name].isel(time=0)

                da = da.rio.set_spatial_dims(
                    x_dim="lon", y_dim="lat", inplace=True
                )
                da = da.rio.write_crs("EPSG:4326", inplace=True)

                # Get the date from the dataset
                date = ds.time.values[0].astype("datetime64[D]").item()

                # Read the GeoDataFrame in chunks to avoid loading too much data at once
                chunk_size = 50

                for start_idx in range(
                    0,
                    len(gpd.read_file(geopackage_path, layer=f"ADM_{level}")),
                    chunk_size,
                ):
                    gdf = gpd.read_file(
                        geopackage_path,
                        layer=f"ADM_{level}",
                        rows=slice(start_idx, start_idx + chunk_size),
                    )

                    gdf = gdf.to_crs("EPSG:4326")

                    gid_column = f"GID_{level}"
                    gdf = gdf[[gid_column, "geometry"]]

                    all_results_len = 0

                    # Determine the optimal batch size and number of processes
                    num_rows = len(gdf)
                    num_processes = min(6, num_rows)
                    batch_size = max(1, num_rows // num_processes)

                    batches = [
                        gdf.iloc[i : i + batch_size]
                        for i in range(0, len(gdf), batch_size)
                    ]

                    process_batch_partial = partial(
                        process_batch, da=da, level=level, date=date
                    )

                    with tqdm(
                        total=len(gdf),
                        desc=f"Overall progress - level {level}",
                    ) as overall_pbar:
                        results = []
                        for batch_result in pool.imap(
                            process_batch_partial, batches
                        ):
                            results.extend(batch_result)
                            overall_pbar.update(
                                len(batch_result)
                                // len(da.attrs["flag_values"])
                            )

                    if results:
                        all_results_len += len(results)
                        insert_data_to_db(results, db_conn)
                        print(
                            f"Inserted {len(results)} rows for {variable_name} at level {level}"
                        )
                        gc.collect()

                # Move the processed file to the nested folder structure
                move_processed_file(file_path, data_directory, level)

                end_time = time.time()

                print(
                    f"\n{variable_name} from file: {file_path} - Level {level} Results:"
                )
                print(
                    f"Processed {all_results_len} rows in {end_time - start_time:.2f} seconds"
                )

        finally:
            ds.close()
            gc.collect()

    print(f"\nProcessing complete for level {level}")
    return all_results_len


def main():
    """
    Main function to process netCDF files and insert data into the PostgreSQL database.
    """
    data_directory = input(
        "Enter the path to the directory containing the netCDF files: "
    )
    geopackage_file_path = input("Enter the path to the GeoPackage file: ")

    db_params = {
        "dbname": "merge",
        "user": "postgres",
        "password": getpass("Enter the database password: "),
        "host": input("Enter the database host: "),
        "port": "5432",
    }

    conn = psycopg2.connect(**db_params)

    levels = [0, 1]
    variable_name = "lccs_class"  # Assuming the variable name to search for

    # Set up Dask client
    client = Client(processes=False, threads_per_worker=6, n_workers=1)

    try:
        for level in levels:
            start_time = time.time()

            print("Using Dask")
            with ProgressBar():
                all_results_len = process_level(
                    geopackage_file_path,
                    level,
                    data_directory,
                    variable_name,
                    conn,
                    use_dask=True,
                )

            end_time = time.time()

            print(f"\nLevel {level} Results:")
            print(
                f"Processed {all_results_len} rows in {end_time - start_time:.2f} seconds"
            )

    finally:
        conn.close()

    client.close()


if __name__ == "__main__":
    main()
