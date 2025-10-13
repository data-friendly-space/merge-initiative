import gc
import glob
import logging
import os  # Import os module to handle file operations
import time
from functools import partial
from getpass import getpass
from multiprocessing import Pool

import dask
import geopandas as gpd
import numpy as np
import psycopg2
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from psycopg2.extras import execute_batch
from rasterio import features
from shapely.geometry import box

# from tqdm import tqdm
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO)


def insert_data_to_db(data, conn, chunk_size=100000):
    """
    Insert data into the PostgreSQL database in chunks.

    :param data: List of tuples containing the data to be inserted
    :param conn: Database connection object
    :param chunk_size: Number of rows to insert in each batch
    """
    cursor = conn.cursor()

    for i in range(0, len(data), chunk_size):
        data_chunk = data[i : i + chunk_size]
        query = """
        INSERT INTO geospatial_data_era5 (gid, admin_level, date, variable, mean, min, max, missing_value_percentage, source, unit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (gid, admin_level, date, variable) 
        DO UPDATE SET mean = EXCLUDED.mean, min = EXCLUDED.min, max = EXCLUDED.max, 
                      missing_value_percentage = EXCLUDED.missing_value_percentage, source = EXCLUDED.source, unit = EXCLUDED.unit
        """
        execute_batch(cursor, query, data_chunk)
        conn.commit()
        print(f"Inserted chunk of {len(data_chunk)} rows.")

    conn.commit()


def calculate_cell_area(da):
    """
    Calculate the area of each cell in the DataArray.
    """
    res_x, res_y = da.rio.resolution()
    area_sq_degrees = np.abs(res_x * res_y)

    earth_radius = 6371000  # meters
    lat_radians = np.deg2rad(da.latitude)

    area = (
        area_sq_degrees
        * (np.pi / 180) ** 2
        * earth_radius**2
        * np.cos(lat_radians)
    )
    area_2d = np.tile(area, (da.sizes["longitude"], 1)).T

    return xr.DataArray(
        area_2d,
        dims=("latitude", "longitude"),
        coords={"latitude": da.latitude, "longitude": da.longitude},
    )


def calculate_cell_fractions(da, geometry, buffer_size=2):
    """
    Calculate cell fractions for the given DataArray and geometry.
    """
    transform = da.rio.transform()
    raster_shape = (da.sizes["latitude"], da.sizes["longitude"])

    rasterized = features.rasterize(
        [geometry],
        out_shape=raster_shape,
        transform=transform,
        all_touched=True,
        dtype=float,
    )

    boundary = np.zeros_like(rasterized, dtype=bool)
    for i in range(-buffer_size, buffer_size + 1):
        for j in range(-buffer_size, buffer_size + 1):
            if i == 0 and j == 0:
                continue
            boundary = np.logical_or(
                boundary,
                np.logical_and(
                    rasterized > 0,
                    np.roll(np.roll(rasterized == 0, i, axis=0), j, axis=1),
                ),
            )

    y_coords, x_coords = np.where(boundary)
    for y, x in zip(y_coords, x_coords):
        x1, y1 = transform * (x, y)
        x2, y2 = transform * (x + 1, y + 1)
        cell = box(min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2))
        intersection = cell.intersection(geometry)
        rasterized[y, x] = intersection.area / cell.area

    result = xr.DataArray(
        rasterized,
        dims=("latitude", "longitude"),
        coords={"latitude": da.latitude, "longitude": da.longitude},
    )

    # # Check for fractional values
    # fractional_cells = ((result > 0) & (result < 1)).sum().item()
    # non_zero_cells = (result != 0).sum().item()
    # total_cells = result.size

    # print(f"Fractional cells: {fractional_cells} out of {total_cells} ({fractional_cells/total_cells*100:.2f}%)")
    # print(f"Non-zero cells: {non_zero_cells} out of {total_cells} ({non_zero_cells/total_cells*100:.2f}%)")

    # # Zoom in on the area of interest
    # lon_min, lat_min, lon_max, lat_max = geometry.bounds
    # lon_buffer = 2  # Add 2 degrees buffer
    # lat_buffer = 2
    # def get_lat_order(da):
    #     """Determine if latitude is in descending (North to South) or ascending (South to North) order."""
    #     return 'descending' if (da.latitude.diff('latitude') < 0).all().item() else 'ascending'

    # lat_order = get_lat_order(da)
    # print("Latitude order:", lat_order, f'{'90:-90' if lat_order == 'descending' else '-90:90'}')
    # zoomed_fractions = result.sel(
    #     longitude=slice(lon_min - lon_buffer, lon_max + lon_buffer),
    #     latitude=slice(lat_max + lat_buffer, lat_min - lat_buffer) if lat_order == 'descending' else slice(lat_min - lat_buffer, lat_max + lat_buffer)
    # )

    # # Plotting
    # plt.figure(figsize=(12, 8))
    # if zoomed_fractions.size > 0 and zoomed_fractions.notnull().any():
    #     zoomed_fractions.plot()
    #     plt.title('Zoomed Cell Fractions')
    # else:
    #     plt.text(0.5, 0.5, "No data to plot", ha='center', va='center')
    #     plt.title('Zoomed Cell Fractions (Empty)')
    # plt.show()

    return result


def calculate_daily_stats(da, geometry, cell_area, buffer_size=2):
    """
    Calculate daily statistics for the given DataArray and geometry.

    :param da: xarray DataArray
    :param geometry: Shapely geometry object
    :param cell_area: xarray DataArray with cell areas
    :param buffer_size: Buffer size for calculating cell fractions
    :return: Tuple of daily mean, min, max, and missing value percentage
    """
    cell_fractions = calculate_cell_fractions(
        da, geometry, buffer_size=buffer_size
    )
    clipped = da.rio.clip([geometry], all_touched=True)

    missing_value_percentage = (
        clipped.isnull().sum() / clipped.size * 100
    ).values

    if clipped.isnull().all():
        return None, None, None, missing_value_percentage

    weights = cell_area * cell_fractions
    weighted_sum = (clipped * weights).sum(dim=["latitude", "longitude"])
    weight_sum = weights.sum(dim=["latitude", "longitude"])
    daily_mean = weighted_sum / weight_sum
    daily_min = clipped.min(dim=["latitude", "longitude"], skipna=True)
    daily_max = clipped.max(dim=["latitude", "longitude"], skipna=True)

    #  # Plot the results
    # if daily_mean is not None and daily_min is not None and daily_max is not None:
    #     plt.figure(figsize=(15, 5))

    #     # Check if there's only one time point
    #     if len(daily_mean.time) == 1:
    #         # Plot single point for each statistic
    #         plt.scatter(daily_mean.time, daily_mean.values, label='Mean', s=50)
    #         plt.scatter(daily_min.time, daily_min.values, label='Min', s=50)
    #         plt.scatter(daily_max.time, daily_max.values, label='Max', s=50)
    #         # Set x-axis to show the single date point clearly
    #         plt.xlim(daily_mean.time.values[0] - np.timedelta64(12, 'h'),
    #                 daily_mean.time.values[0] + np.timedelta64(12, 'h'))
    #     else:
    #         # Plot lines if there are multiple time points
    #         plt.plot(daily_mean.time, daily_mean.values, label='Mean')
    #         plt.plot(daily_min.time, daily_min.values, label='Min')
    #         plt.plot(daily_max.time, daily_max.values, label='Max')

    #     plt.legend()
    #     plt.title('Daily Statistics for ERA5 - 2m Temperature')
    #     plt.xlabel('Date')
    #     plt.ylabel('Temperature (K)')

    #     plt.show()
    # else:
    #     print("No data available for plotting. The selected area might not have any valid data.")

    return daily_mean, daily_min, daily_max, missing_value_percentage


def process_geometry(
    da_daily, variable_name, geometry, level, gid, cell_area, unit
):
    """
    Process a single geometry to calculate daily statistics and prepare data for database insertion.

    :param da_daily: xarray DataArray with daily data
    :param variable_name: Name of the variable
    :param geometry: Shapely geometry object
    :param level: Administrative level
    :param gid: Geometry ID
    :param cell_area: xarray DataArray with cell areas
    :param unit: Unit of the variable
    :return: List of tuples with processed data
    """
    daily_mean, daily_min, daily_max, missing_value_percentage = (
        calculate_daily_stats(da_daily, geometry, cell_area)
    )

    results = []
    for date in da_daily.time.values:
        date_py = date.astype("M8[ms]").astype("O")
        if daily_mean is None:
            results.append(
                (
                    gid,
                    level,
                    date_py,
                    variable_name,
                    None,  # mean
                    None,  # min
                    None,  # max
                    100.0,  # missing_value_percentage (100% when no data)
                    "ERA5",
                    unit,
                )
            )
        else:
            mean_val = daily_mean.sel(time=date).values
            min_val = daily_min.sel(time=date).values
            max_val = daily_max.sel(time=date).values
            results.append(
                (
                    gid,
                    level,
                    date_py,
                    variable_name,
                    float(mean_val),
                    float(min_val),
                    float(max_val),
                    round(float(missing_value_percentage), 2),
                    "ERA5",
                    unit,
                )
            )

    return results


def process_batch(batch, da_daily, var_name, level, cell_area, unit):
    """
    Process a batch of geometries to calculate daily statistics and prepare data for database insertion.

    :param batch: GeoDataFrame with geometries
    :param da_daily: xarray DataArray with daily data
    :param var_name: Variable name
    :param level: Administrative level
    :param cell_area: xarray DataArray with cell areas
    :param unit: Unit of the variable
    :return: List of tuples with processed data
    """
    results = []
    gid_column = f"GID_{level}"
    # Create a progress bar for this batch
    with tqdm(
        total=len(batch),
        desc=f"Processing batch - {var_name} - level {level}",
        leave=False,
    ) as pbar:
        for _, row in batch.iterrows():
            result = process_geometry(
                da_daily,
                var_name,
                row["geometry"],
                level,
                row[gid_column],
                cell_area,
                unit,
            )
            results.extend(
                result
            )  # Always extend results, even if they contain null values
            pbar.update(1)  # Update progress bar for each row
    return results


def process_level(
    geopackage_path, level, data_directory, variables, db_conn, use_dask=False
):
    """
    Process a specific administrative level to calculate daily statistics and insert data into the database.
    """
    print(f"Processing level {level}")

    gdf = gpd.read_file(geopackage_path, layer=f"ADM_{level}")
    gdf = gdf.to_crs("EPSG:4326")

    gid_column = f"GID_{level}"
    gdf = gdf[[gid_column, "geometry"]]

    all_results_len = 0

    # Determine the optimal batch size and number of processes
    num_rows = len(gdf)
    num_processes = min(
        6, num_rows
    )  # Use at most 6 processes, but not more than the number of rows
    batch_size = max(
        1, num_rows // num_processes
    )  # Ensure at least one item per batch

    # Create a single process pool outside the loops
    with Pool(processes=num_processes) as pool:
        for var_code, var_name in variables.items():
            file_pattern = (
                f"{data_directory}/*_{var_name}_daily_aggregated*.nc"
            )
            matching_files = glob.glob(file_pattern)

            if not matching_files:
                print(f"No file found for {var_name}")
                continue

            matching_files.sort()

            for file_path in matching_files:
                print(f"Processing {var_name} from file: {file_path}")

                start_time = time.time()

                if use_dask:
                    with dask.config.set(
                        **{"array.slicing.split_large_chunks": True}
                    ):
                        ds_daily = xr.open_dataset(
                            file_path,
                            chunks={
                                "time": 1,
                                "latitude": 500,
                                "longitude": 500,
                            },
                        )
                        da_daily = ds_daily[var_code]
                else:
                    ds_daily = xr.open_dataset(file_path)
                    da_daily = ds_daily[var_code]

                da_daily = da_daily.rio.set_spatial_dims(
                    x_dim="longitude", y_dim="latitude", inplace=True
                )
                da_daily = da_daily.rio.write_crs("EPSG:4326", inplace=True)

                cell_area = calculate_cell_area(da_daily)
                unit = da_daily.attrs.get("units", "unknown")

                try:
                    batches = [
                        gdf.iloc[i : i + batch_size]
                        for i in range(0, len(gdf), batch_size)
                    ]

                    process_batch_partial = partial(
                        process_batch,
                        da_daily=da_daily,
                        var_name=var_name,
                        level=level,
                        cell_area=cell_area,
                        unit=unit,
                    )

                    # Use tqdm to show overall progress for all batches
                    with tqdm(
                        total=len(gdf),
                        desc=f"Overall progress: {var_name} - level {level}",
                    ) as overall_pbar:
                        results = []
                        for batch_result in pool.imap(
                            process_batch_partial, batches
                        ):
                            results.extend(batch_result)
                            overall_pbar.update(len(batch_result))

                    flat_results = [item for item in results if item]

                    if flat_results:
                        all_results_len += len(flat_results)
                        insert_data_to_db(flat_results, db_conn)
                        print(
                            f"Inserted {len(flat_results)} rows for {var_name} at level {level}"
                        )

                        # Move the processed file to another folder
                        processed_folder = os.path.join(
                            data_directory, "processed"
                        )
                        os.makedirs(processed_folder, exist_ok=True)
                        os.rename(
                            file_path,
                            os.path.join(
                                processed_folder, os.path.basename(file_path)
                            ),
                        )
                        print(
                            f"Moved processed file {file_path} to {processed_folder}"
                        )

                finally:
                    ds_daily.close()
                    gc.collect()

                end_time = time.time()

                print(
                    f"\n{var_name} from file: {file_path} - Level {level} Results:"
                )
                print(
                    f"Processed {all_results_len} rows in {end_time - start_time:.2f} seconds"
                )

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

    variables = {
        "tp": "total_precipitation",
        "e": "evaporation",
        "t2m": "2m_temperature",
        "mx2t": "maximum_2m_temperature_since_previous_post_processing",
        "mn2t": "minimum_2m_temperature_since_previous_post_processing",
        "ssr": "surface_net_solar_radiation",
    }

    db_params = {
        "dbname": "merge",
        "user": "postgres",
        "password": getpass("Enter the database password: "),
        "host": input("Enter the database host: "),
        "port": "5432",
    }

    conn = psycopg2.connect(**db_params)

    levels = [0, 1]

    # Set up Dask client
    client = Client(processes=False, threads_per_worker=6, n_workers=1)

    try:
        for level in levels:
            start_time = time.time()

            if level < 2:
                print("not Using Dask")
                all_results_len = process_level(
                    geopackage_file_path,
                    level,
                    data_directory,
                    variables,
                    conn,
                    use_dask=False,
                )
            else:
                print("Using Dask")
                with ProgressBar():
                    all_results_len = process_level(
                        geopackage_file_path,
                        level,
                        data_directory,
                        variables,
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
