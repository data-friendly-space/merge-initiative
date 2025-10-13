import glob
import os
import shutil
from datetime import datetime
from getpass import getpass
from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import psycopg2
import rasterio
from psycopg2.extras import execute_batch
from pyhdf.SD import SD, SDC
from rasterio.features import geometry_mask
from tqdm import tqdm


def calculate_ndvi(file_path):
    """Calculate NDVI from HDF file using pyhdf."""
    hdf = SD(file_path, SDC.READ)

    # Read the quality data first
    quality = hdf.select("Albedo_Quality").get()

    # Create a mask for high-quality pixels
    quality_mask = quality <= 5

    # Read and pre-filter the reflectance data
    red = hdf.select("Nadir_Reflectance_Band1").get().astype(float)
    nir = hdf.select("Nadir_Reflectance_Band2").get().astype(float)

    # Apply quality mask
    red[~quality_mask] = np.nan
    nir[~quality_mask] = np.nan

    # Correct scale factor
    scale_factor = 0.001

    # Apply scale factor and handle fill values
    fill_value = 32767
    red[red == fill_value] = np.nan
    nir[nir == fill_value] = np.nan

    red *= scale_factor
    nir *= scale_factor

    # Calculate NDVI
    with np.errstate(divide="ignore", invalid="ignore"):
        ndvi = (nir - red) / (nir + red)

    # Handle invalid values
    ndvi = np.where((nir + red) != 0, ndvi, np.nan)

    # Clip NDVI to valid range
    ndvi = np.clip(ndvi, -1, 1)

    # Get geotransform information
    metadata = hdf.attributes()["StructMetadata.0"]
    uly_line = next(
        line for line in metadata.split("\n") if "UpperLeftPointMtrs" in line
    )
    lry_line = next(
        line for line in metadata.split("\n") if "LowerRightMtrs" in line
    )
    ulx, uly = map(float, uly_line.split("=")[1].strip("()").split(","))
    lrx, lry = map(float, lry_line.split("=")[1].strip("()").split(","))

    height, width = ndvi.shape
    res_x = (lrx - ulx) / width
    res_y = (lry - uly) / height
    transform = rasterio.transform.from_origin(ulx, uly, res_x, abs(res_y))

    hdf.end()
    return ndvi, transform


def calculate_cell_areas(transform, shape):
    """Calculate the area of each cell in the raster using a simple spherical approximation."""
    res_x = abs(transform[0])
    res_y = abs(transform[4])
    area_sq_degrees = res_x * res_y

    earth_radius = 6371000  # meters

    # Calculate latitudes for each row
    lats = np.linspace(
        transform[3] + transform[4] * shape[0],  # bottom latitude
        transform[3],  # top latitude
        shape[0],
    )

    lat_radians = np.deg2rad(lats)

    # Calculate areas for each row
    areas = (
        area_sq_degrees
        * (np.pi / 180) ** 2
        * earth_radius**2
        * np.cos(lat_radians)
    )

    # Create 2D array of areas
    area_2d = np.tile(areas[:, np.newaxis], (1, shape[1]))

    return area_2d


def calculate_zonal_stats(ndvi, transform, geometry):
    """Calculate area-weighted zonal statistics for a given geometry."""
    with rasterio.Env():
        mask = geometry_mask(
            [geometry], ndvi.shape, transform, invert=True, all_touched=True
        )

    cell_areas = calculate_cell_areas(transform, ndvi.shape)

    masked_ndvi = np.where(mask, ndvi, np.nan)
    masked_areas = np.where(mask, cell_areas, 0)

    valid_mask = ~np.isnan(masked_ndvi)
    valid_ndvi = masked_ndvi[valid_mask]
    valid_areas = masked_areas[valid_mask]

    if valid_ndvi.size == 0:
        return None, None, None, 100.0

    total_area = np.sum(valid_areas)
    weighted_sum = np.sum(valid_ndvi * valid_areas)
    weighted_mean = weighted_sum / total_area

    min_val = np.min(valid_ndvi)
    max_val = np.max(valid_ndvi)
    # sum_val = np.sum(valid_ndvi)

    total_country_pixels = np.sum(mask)
    valid_country_pixels = np.sum(valid_mask)
    missing_percentage = (
        (total_country_pixels - valid_country_pixels) / total_country_pixels
    ) * 100

    return (
        float(weighted_mean),
        float(min_val),
        float(max_val),
        round(float(missing_percentage), 2),
    )


def process_geometry(args):
    """Process a single geometry from the GeoDataFrame."""
    ndvi, transform, row, level, date = args
    stats = calculate_zonal_stats(ndvi, transform, row["geometry"])
    if stats:
        # if stats[0] is None:
        # print(f"{row['GID_0']} at level {level} NDVI calculation is None")
        return (
            row[f"GID_{level}"],
            level,
            date,
            "NDVI",
            *stats,
            "NASA_MCD43C4",
            "unitless",
        )
    else:
        print("error with stats")
        return None


def process_file(file_path, gdf, level, conn):
    """Process a single HDF file for all geometries."""
    ndvi, transform = calculate_ndvi(file_path)

    # Extract date from filename and convert to YYYY-MM-DD
    date_str = os.path.basename(file_path).split(".")[1][
        1:
    ]  # Extract YYYY001 format
    date = datetime.strptime(date_str, "%Y%j").strftime("%Y-%m-%d")

    with Pool(processes=6) as pool:
        args = [
            (ndvi, transform, row, level, date) for _, row in gdf.iterrows()
        ]
        results = list(
            tqdm(
                pool.imap(process_geometry, args),
                total=len(args),
                desc=f"Processing geometries for {file_path}",
            )
        )

    results = [r for r in results if r is not None]

    if results:
        insert_data_to_db(results, conn)
        print(f"Inserted {len(results)} rows for file {file_path}")

    return file_path


def insert_data_to_db(data, conn):
    """Insert data into the PostgreSQL database."""
    cursor = conn.cursor()
    query = """
    INSERT INTO geospatial_data_nvdi (gid, admin_level, date, variable, mean, min, max, missing_value_percentage, source, unit)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (gid, admin_level, date, variable) 
    DO UPDATE SET mean = EXCLUDED.mean, min = EXCLUDED.min, max = EXCLUDED.max,
                  missing_value_percentage = EXCLUDED.missing_value_percentage, source = EXCLUDED.source, unit = EXCLUDED.unit
    """
    execute_batch(cursor, query, data)
    conn.commit()


def find_files(data_directory):
    """
    Find all relevant HDF files, including those in processed folders.

    :param data_directory: Base directory for data files
    :return: List of file paths
    """
    file_pattern = "MCD43C4.A*.hdf"

    # Find files in the main directory
    main_files = glob.glob(os.path.join(data_directory, file_pattern))

    # Find files in processed folders
    processed_files = []
    for level in range(2):  # We have levels 0 and 1
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


def main():
    data_directory = input(
        "Enter the path to the directory containing the HDF files: "
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

    levels = [0, 1]  # Process for admin levels 0, 1, and 2

    try:
        for level in levels:
            print(f"Processing level {level}")
            gdf = gpd.read_file(geopackage_file_path, layer=f"ADM_{level}")
            gdf = gdf.to_crs("EPSG:4326")

            file_list = find_files(data_directory)

            for file_path in tqdm(
                file_list, desc=f"Processing files for level {level}"
            ):
                if get_processed_level(file_path, data_directory) < level:
                    processed_file = process_file(file_path, gdf, level, conn)
                    move_processed_file(processed_file, data_directory, level)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
