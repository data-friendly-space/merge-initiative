import os
import sys
project_root = os.environ.get('PROJECT_ROOT') # run this in terminal before executing the script: export PROJECT_ROOT=/path/to/your/project/root
print(project_root)
sys.path.append(str(project_root))
from config_loader import CONFIG

import pandas as pd
import os
import psycopg2
import re
import csv
import numpy as np
import json


def extract_locations(region):
    """
    Extract individual location names from the region string.
    
    Args:
    region (str): The region string to process.
    
    Returns:
    list: A list of extracted location names.
    """
    # Remove 'County of ' from the string
    region = region.replace('County of ', '')
    
    # Replace "and" with comma
    region = region.replace(' and ', ', ')
    
    # Handle cases like "Central (Kabul Wardak Kapisa Logar Parwan Panjsher)"
    if '(' in region and ')' in region:
        locations = re.findall(r'\(([^)]+)\)', region)
        if locations:
            if ',' in locations[0]:
                locations = [loc.strip() for loc in locations[0].split(',')]
            else:
                locations = locations[0].split()
    else:
        # Split the string by commas
        locations = [loc.strip() for loc in region.split(',') if loc.strip()]
    
    # Further processing
    final_locations = []
    for loc in locations:
        # Strip 'region' from the end
        loc = re.sub(r'\s*region$', '', loc, flags=re.IGNORECASE)
        
        # Strip Roman numerals from the beginning
        loc = re.sub(r'^[IVX]+-\s*', '', loc)
        
        # Remove 'incl' if it's at the start
        loc = re.sub(r'^incl\.?\s*', '', loc)
        
        # Ignore single words that are directions, "total", percentages, or specific categories
        ignore_words = ['', 'incl', 'total', 'north', 'south', 'east', 'west', 'western', 'eastern', 'northern', 'southern', 'central', 'north-western', 'north-eastern', 'south-western', 'south-eastern', 'north west', 'north east', 'south west', 'south east', 'north-west', 'north-east', 'south-west', 'south-east', 'second 25%', 'third 25%', 'lowest 25%', 'highest 25%', 'urban', 'rural', 'poor', 'nonpoor']
        
        if loc.lower() not in ignore_words and not any(percentage in loc.lower() for percentage in ['25%', '50%', '75%', '100%']) and not any(direction in loc.lower() for direction in ['north-west', 'north-east', 'south-west', 'south-east']):
            final_locations.append(loc)
    
    return final_locations

def normalize_name(name):
    """
    Normalize a location name by converting to lowercase, removing special characters,
    and replacing spaces with underscores.
    
    Args:
    name (str): The name to normalize.
    
    Returns:
    str: The normalized name.
    """
    # Convert to lowercase
    normalized = name.lower()
    # Remove special characters
    normalized = re.sub(r'[^a-z0-9\s]', '', normalized)
    # Replace spaces with underscores
    normalized = normalized.replace(' ', '_')
    return normalized

def get_unique_admin_names(table_name, main_column, var_name_column, nl_name_column, gid_column, iso_column):
    """
    Retrieve unique administrative names and their corresponding GIDs and ISO3 codes from a specified table and columns in the database.
    
    Args:
    table_name (str): Name of the table to query.
    main_column (str): Name of the main column containing admin names.
    var_name_column (str): Name of the column containing variant names.
    nl_name_column (str): Name of the column containing native language names.
    gid_column (str): Name of the column containing GID.
    iso_column (str): Name of the column containing ISO3 code.
    
    Returns:
    dict: Dictionary of normalized unique admin names as keys and tuples of (GID, ISO3) as values.
    """
    conn_params = CONFIG['LOCAL_DB_CONFIG']
    unique_names = {}
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Query for main names
                cur.execute(f"SELECT DISTINCT {main_column}, {gid_column}, {iso_column} FROM {table_name};")
                unique_names.update({normalize_name(row[0]): (row[1], row[2]) for row in cur.fetchall() if row[0]})
                
                # Query for variant names
                cur.execute(f"SELECT DISTINCT unnest({var_name_column}), {gid_column}, {iso_column} FROM {table_name};")
                unique_names.update({normalize_name(row[0]): (row[1], row[2]) for row in cur.fetchall() if row[0]})
                
                # Query for native language names
                cur.execute(f"SELECT DISTINCT unnest({nl_name_column}), {gid_column}, {iso_column} FROM {table_name};")
                unique_names.update({normalize_name(row[0]): (row[1], row[2]) for row in cur.fetchall() if row[0]})
        
        return unique_names
    except psycopg2.Error as e:
        print(f"Error querying {table_name}: {e}")
        return {}


# Get unique admin names, GIDs, and ISO3 codes from new tables
unique_admin1_names = get_unique_admin_names('gadm_admin1_new', 'admin_level_1', 'admin_level_1_var_name', 'admin_level_1_nl_name', 'gid_1', 'iso3')
unique_admin2_names = get_unique_admin_names('gadm_admin2_new', 'admin_level_2', 'admin_level_2_var_name', 'admin_level_2_nl_name', 'gid_2', 'iso3')

def read_gdl_csv_files(folder_path, file_name_contains):
    """
    Read all GDL CSV files from the specified folder that contain the specified string in their filename.
    
    Args:
    folder_path (str): Path to the folder containing GDL CSV files.
    file_name_contains (str): String to filter filenames.
    
    Returns:
    list: List of pandas DataFrames, each containing data from a GDL CSV file.
    """
    dataframes = []
    
    # Iterate through all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv') and file_name_contains.lower() in filename.lower():
            file_path = os.path.join(folder_path, filename)
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Add the filename as a column to identify the source
            df['source_file'] = filename
            
            dataframes.append(df)
    
    return dataframes

def process_gdl_data(dataframes):
    """
    Process GDL data by combining DataFrames and mapping ISO codes.
    
    Args:
    dataframes (list): List of pandas DataFrames containing GDL data.
    
    Returns:
    pandas.DataFrame: Processed DataFrame with combined data and mapped ISO codes.
    """
    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Map ISO codes if necessary (based on the investigation results)
    iso_map = {}  # Add any necessary mappings here
    if iso_map:
        combined_df['iso_code'] = combined_df['iso_code'].map(lambda x: iso_map.get(x, x))
    
    return combined_df

def prepare_data_for_insertion(df):
    """
    Prepare data for insertion into the database.
    
    Args:
    df (pandas.DataFrame): Input DataFrame.
    
    Returns:
    list: List of tuples ready for database insertion.
    """
    data_to_insert = []
    variables = [
        'iwi', 'iwipov70', 'iwipov50', 'iwipov35', 'internet', 'cellphone',
        'thtwithin', 'thtbetween', 'urban', 'edyr25', 'womedyr25', 'menedyr25',
        'workwom', 'wagri', 'wwrklow', 'wwrkhigh', 'hagri', 'hwrklow', 'hwrkhigh',
        'agedifmar', 'agemarw20', 'tfr', 'stunting', 'haz', 'whz', 'waz', 'bmiz',
        'dtp3age1', 'measlage1', 'regpopm', 'popshare', 'age09', 'age1019',
        'age2029', 'age3039', 'age4049', 'age5059', 'age6069', 'age7079',
        'age8089', 'age90hi', 'hhsize', 'popworkage', 'popold', 'infmort',
        'u5mort', 'pipedwater', 'electr'
    ]
    
    # Convert all variables to numeric type, replacing empty or space with NaN
    for variable in variables:
        df[variable] = pd.to_numeric(df[variable].replace(r'^\s*$', np.nan, regex=True), errors='coerce')

    # Set to store unique combinations of ISO Code, Location, and Original Region
    unique_unmatched_locations = set()
    
    for _, row in df.iterrows():
        extracted_locations = extract_locations(row['region'])
        
        # Create metadata JSON
        metadata = json.dumps({
            'iso_code': row['iso_code'] if pd.notna(row['iso_code']) else None,
            'ISO2': row['ISO2'] if pd.notna(row['ISO2']) else None,
            'iso_num': row['iso_num'] if pd.notna(row['iso_num']) else None,
            'country': row['country'] if pd.notna(row['country']) else None,
            'year': row['year'] if pd.notna(row['year']) else None,
            'datasource': row['datasource'] if pd.notna(row['datasource']) else None,
            'GDLCODE': row['GDLCODE'] if pd.notna(row['GDLCODE']) else None,
            'level': row['level'] if pd.notna(row['level']) else None,
            'region': row['region'] if pd.notna(row['region']) else None
        })
        
        for variable in variables:
            value = row[variable]
            # Standardize 'regpopm' from millions to persons
            if variable == 'regpopm' and not pd.isna(value):
                value *= 1000000
            
            # Cast the value to a Python float to avoid np.float64
            if not pd.isna(value):
                value = float(value)
            else:
                value = None
            
            # Determine admin level and note
            if row['level'] == 'National':
                admin_level = 0
                note = None
                data_to_insert.append((
                    row['iso_code'],
                    admin_level,
                    f"{row['year']}-01-01",
                    variable,
                    value,
                    note,
                    row['source_file'],  # Add source file name
                    metadata  # Add metadata JSON
                ))
            else:
                for location in extracted_locations:
                    admin_level = None
                    note = None if len(extracted_locations) == 1 else f"Extracted from: {row['region']}"
                    gid = None

                    # Normalize the location name
                    normalized_location = normalize_name(location)
                    
                    # Check GADM table for matches in level 1 and 2, considering ISO3 code
                    if normalized_location in unique_admin1_names and unique_admin1_names[normalized_location][1] == row['iso_code']:
                        admin_level = 1
                        gid = unique_admin1_names[normalized_location][0]
                    elif normalized_location in unique_admin2_names and unique_admin2_names[normalized_location][1] == row['iso_code']:
                        admin_level = 2
                        gid = unique_admin2_names[normalized_location][0]
                    
                    if admin_level:
                        data_to_insert.append((
                            gid,
                            admin_level,
                            f"{row['year']}-01-01",
                            variable,
                            value,
                            note,
                            row['source_file'],  # Add source file name
                            metadata  # Add metadata JSON
                        ))
                    else:
                        # If location doesn't match, add to unmatched_locations set
                        unique_unmatched_locations.add((row['iso_code'], location, row['region']))
        
    # Write unique unmatched locations to a CSV file
    if unique_unmatched_locations:
        unmatched_file = 'unmatched_locations_Area.csv'
        with open(unmatched_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ISO Code', 'Location', 'Original Region'])
            writer.writerows(unique_unmatched_locations)
    
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
    INSERT INTO geospatial_data_gdl (gid, admin_level, date, variable, raw_value, note, source, metadata)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (gid, admin_level, date, variable) DO UPDATE
    SET raw_value = EXCLUDED.raw_value,
        note = EXCLUDED.note,
        source = EXCLUDED.source,
        metadata = EXCLUDED.metadata
    """
    cur.executemany(insert_query, data)
    return len(data)

def main(gdl_folder, db_config):
    """
    Main function to orchestrate the ETL process for GDL Area data.
    
    Args:
    gdl_folder (str): Path to the GDL CSV files.
    db_config (dict): Database configuration parameters.
    """
    # Read GDL files
    gdl_data = read_gdl_csv_files(gdl_folder, file_name_contains="Area")
    print(f"Number of GDL files read: {len(gdl_data)}")

    # Process GDL data
    combined_df = process_gdl_data(gdl_data)
    print(f"Number of rows after processing: {len(combined_df)}")

    # Prepare data for insertion
    data_to_insert = prepare_data_for_insertion(combined_df)

    # Connect to the database and insert data
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            rows_inserted = insert_data_to_db(cur, data_to_insert)
            print(f"Inserted {rows_inserted} rows into geospatial_data_gdl table.")

if __name__ == "__main__":
    # Use the configuration
    gdl_folder = CONFIG['GDL_FOLDER']
    db_config = CONFIG['LOCAL_DB_CONFIG']
    main(gdl_folder, db_config)