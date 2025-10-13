import os
import sys
project_root = os.environ.get('PROJECT_ROOT') # run this in terminal before executing the script: export PROJECT_ROOT=/path/to/your/project/root
print(project_root)
sys.path.append(str(project_root))
from config_loader import CONFIG

import pandas as pd
import os
import psycopg2
import json

def read_idmc_files(folder_path, sheet_name):
    """
    Read all IDMC excel files from the specified folder, focusing on the specified sheet.
    
    Args:
    folder_path (str): Path to the folder containing IDMC excel files.
    sheet_name (str): Name of the sheet to read from each Excel file.
    
    Returns:
    list: List of pandas DataFrames, each containing data from the specified sheet of an IDMC excel file.
    """
    dataframes = []
    
    # Iterate through all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.xlsx'):  # Check if the file is an Excel file
            file_path = os.path.join(folder_path, filename)
            
            # Read the Excel file, specifically the specified sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Add the filename as a column to identify the source
            df['source_file'] = f"{filename}_{sheet_name}"
            
            dataframes.append(df)
    
    return dataframes

def process_idmc_data(dataframes):
    """
    Process IDMC data by combining DataFrames, removing duplicates, and mapping ISO3 codes.
    
    Args:
    dataframes (list): List of pandas DataFrames containing IDMC data.
    
    Returns:
    pandas.DataFrame: Processed DataFrame with combined data and mapped ISO3 codes.
    """
    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Remove duplicates
    combined_df = combined_df.drop_duplicates(subset=['ISO3', 'Year', 'Sex', 'Cause'], keep='first')
    
    # Map ISO3 codes
    iso_map = {'XKX': "XKO", 'AB9': "SDN", 'HKG': "CHN", 'MAC': "CHN"}
    combined_df['ISO3'] = combined_df['ISO3'].map(lambda x: iso_map.get(x, x))
    
    return combined_df

def group_variables(df, variables):
    """
    Group data by ISO3, Country, Year, Sex, Cause, and source_file, and sum the specified variables.

    This function aggregates the input DataFrame by grouping on ISO3, Country, Year, Sex, Cause, and source_file,
    then sums the values for the specified variables within each group.

    Args:
    df (pandas.DataFrame): Input DataFrame containing IDMC data.
    variables (list): List of variable names to sum during grouping.

    Returns:
    pandas.DataFrame: A new DataFrame with data grouped by ISO3, Country, Year, Sex, Cause, and source_file,
                      with summed values for the specified variables.
    """
    grouped_df = df.groupby(['ISO3', 'Country', 'Year', 'Sex', 'Cause', 'source_file'])[variables].sum().reset_index()
    return grouped_df

def prepare_data_for_insertion(df, variables):
    """
    Prepare data for insertion into the database.
    
    Args:
    df (pandas.DataFrame): Input DataFrame.
    variables (list): List of variables to process.
    
    Returns:
    list: List of tuples ready for database insertion.
    """
    data_to_insert = []
    for _, row in df.iterrows():

        # Create metadata JSON
        metadata = json.dumps({
            'ISO3': row['ISO3'] if pd.notna(row['ISO3']) else None,
            'Country': row['Country'] if pd.notna(row['Country']) else None,
            'Year': row['Year'] if pd.notna(row['Year']) else None,
            'Sex': row['Sex'] if pd.notna(row['Sex']) else None,
            'Cause': row['Cause'] if pd.notna(row['Cause']) else None
        })

        for variable in variables:
            data_to_insert.append((
                row['ISO3'],
                0,
                f"{row['Year']}-01-01",
                f"{row['Cause']}_{row['Sex']}_{variable}",
                row[variable],
                row['source_file'],
                metadata
            ))
    return data_to_insert

def insert_data_to_db(cur, data):
    """
    Insert data into the database.
    
    Args:
    cur (psycopg2.cursor): Database cursor.
    data (list): List of tuples to insert.
    
    Returns:
    int: Number of rows inserted.
    """
    insert_query = """
    INSERT INTO geospatial_data_idmc (gid, admin_level, date, variable, raw_value, source, metadata)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (gid, admin_level, date, variable) DO UPDATE
    SET raw_value = EXCLUDED.raw_value, 
        source = EXCLUDED.source,
        metadata = EXCLUDED.metadata
    """
    cur.executemany(insert_query, data)
    return len(data)

def main(idmc_folder, db_config):
    """
    Main function to orchestrate the ETL process.
    
    Args:
    idmc_folder (str): Path to the IDMC excel files.
    db_config (dict): Database configuration parameters.
    """
    # Read IDMC files
    sheet_name = '3_IDPs_SADD_estimates'
    idmc_data = read_idmc_files(idmc_folder, sheet_name=sheet_name)
    print(f"Number of IDMC files read: {len(idmc_data)}")

    # Process IDMC data
    combined_df = process_idmc_data(idmc_data)
    print(f"Number of rows after processing: {len(combined_df)}")

    # Define variables to process
    variables = ["0-4", "5-11", "12-17", "18-59", "60+"]

    # Group variables
    grouped_df = group_variables(combined_df, variables)

    # Prepare data for insertion
    data_to_insert = prepare_data_for_insertion(grouped_df, variables)

    # Connect to the database and insert data
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            rows_inserted = insert_data_to_db(cur, data_to_insert)
            print(f"Inserted {rows_inserted} rows into geospatial_data_idmc table.")

if __name__ == "__main__":
    # Use the configuration
    idmc_folder = CONFIG['IDMC_FOLDER']
    db_config = CONFIG['LOCAL_DB_CONFIG']
    main(idmc_folder, db_config)