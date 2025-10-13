SQL = """
-- Create a table for storing aggregated geospatial data
CREATE TABLE IF NOT EXISTS geospatial_data_worldpop_pwd (
    id SERIAL PRIMARY KEY,
    gid VARCHAR(15) NOT NULL,
    admin_level INTEGER NOT NULL,
    date DATE NOT NULL,
    variable VARCHAR(50) NOT NULL,
    sum NUMERIC,
    mean NUMERIC,
    min NUMERIC,
    max NUMERIC,
    raw_value NUMERIC,
    note TEXT,
    source TEXT,
    metadata JSONB
);

-- Add a unique constraint to ensure no duplicate entries
ALTER TABLE geospatial_data_worldpop_pwd
ADD CONSTRAINT unique_worldpop_pwd_entry UNIQUE (gid, admin_level, date, variable);
"""

from getpass import getpass

import psycopg2


def create_worldpop_pwd_table():
    """
    Create the WorldPop-PWD data table in the local PostgreSQL database using the predefined SQL.
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

        print(
            "WorldPop-PWD data table created successfully with unique constraint."
        )

    except (Exception, psycopg2.Error) as error:
        print(
            f"Error creating WorldPop-PWD data table or adding constraint: {error}"
        )

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()


# Call the function to create the table and add the constraint
create_worldpop_pwd_table()
