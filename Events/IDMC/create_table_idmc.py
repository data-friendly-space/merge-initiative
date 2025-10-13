SQL = """
-- Create a table for storing EM-DAT event data
CREATE TABLE IF NOT EXISTS events_idmc (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    disaster_type VARCHAR(50),
    disaster_subtype VARCHAR(50),
    iso3_code CHAR(3),
    admin_level_0 VARCHAR(100),
    admin_level_1 TEXT[],
    admin_level_2 TEXT[],
    start_date DATE,
    disaster_internal_displacements INTEGER,
    source VARCHAR(50),
    metadata JSONB,
    GLIDE TEXT[],
    local_Identifier TEXT[],
    IFRC_Appeal_ID TEXT[],
    Government_Assigned_Identifier TEXT[]
);

-- Add a unique constraint to ensure no duplicate entries
ALTER TABLE events_idmc
ADD CONSTRAINT unique_event_idmc_entry UNIQUE (event_name, iso3_code, start_date);
"""

from getpass import getpass

import psycopg2


def create_idmc_table():
    """
    Create the IDMC events table in the local PostgreSQL database using the predefined SQL.
    This function also adds a unique constraint to prevent duplicate entries.
    """
    try:
        # Connect to the local PostgreSQL database
        conn = psycopg2.connect(
            dbname="merge",
            user="postgres",
            password=getpass("Enter the database password: "),
            host=input("Enter the database host: "),
        )

        # Create a cursor object
        cur = conn.cursor()

        # Execute the SQL statement to create the table and add the constraint
        cur.execute(SQL)

        # Commit the changes
        conn.commit()

        print("IDMC events table created successfully with unique constraint.")

    except (Exception, psycopg2.Error) as error:
        print(
            f"Error creating IDMC events table or adding constraint: {error}"
        )

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()


# Call the function to create the table and add the constraint
create_idmc_table()
