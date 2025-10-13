# Events Data Processing Pipeline

This directory contains the ETL pipeline for processing and merging disaster event data from multiple sources into a unified PostgreSQL database. The pipeline standardizes event records, administrative locations, disaster classifications, and external identifiers to create a comprehensive events database for analysis.

---

## Table of Contents

1. [Overview](#overview)
2. [Data Sources](#data-sources)
3. [Administrative Area Mapping](#administrative-area-mapping)
4. [Data Preprocessing](#data-preprocessing)
5. [Database Schema](#database-schema)
6. [Data Merging](#data-merging)
7. [Views and Indexes](#views-and-indexes)
8. [Usage Instructions](#usage-instructions)

---

## Overview

The Events pipeline integrates disaster event data from two primary sources:

- **EM-DAT (Emergency Events Database)**: Global disaster events with comprehensive impact metrics
- **IDMC (Internal Displacement Monitoring Centre)**: Internal displacement data linked to disaster events

**Key Features:**

- Standardizes administrative area names to GADM geography
- Normalizes disaster classifications across sources
- Merges events using GLIDE code matching
- Generates unique event identifiers
- Stores flexible metadata in JSONB columns
- Creates spatial views for geospatial analysis

**Processing Flow:**

```
1. EM-DAT Raw Data → preprocess_emdat.ipynb → events_emdat table
2. IDMC Raw Data → preprocess_idmc.ipynb → events_idmc table
3. events_emdat + events_idmc → merge_events_with_seperate_events_table.ipynb → events table
4. events table → Views (events_flattened_admin, events_with_geometry)
```

---

## Data Sources

### EM-DAT (Emergency Events Database)

**Source:** https://public.emdat.be/

**Access Method:** GraphQL API with authentication

**File Format:** Excel (.xlsx)

**Key Variables:**

- Disaster classification (group, subgroup, type, subtype)
- Impact metrics (deaths, injuries, affected population, homeless)
- Economic data (total damage, reconstruction costs, aid contribution)
- Geographic data (ISO3, country, admin units, coordinates)
- External identifiers (GLIDE, USGS, DFO)
- Temporal data (start/end dates)

### IDMC (Internal Displacement Monitoring Centre)

**Source:** https://www.internal-displacement.org/database/displacement-data/

**Access Method:** Direct HTTP download

**File Format:** Excel (.xlsx)

**Key Variables:**

- Disaster classification (hazard type, hazard subtype)
- Displacement figures (disaster internal displacements)
- Geographic data (ISO3, country/territory)
- Event identifiers (GLIDE, Local Identifier, IFRC Appeal ID, Government Identifier)
- Event name (contains location information)
- Temporal data (year, event start date)

---

## Administrative Area Mapping

Both data sources require standardization of location names to match GADM administrative boundaries. This is accomplished through mapping files and transformation rules.

### Mapping Files

**`EMDAT_admin_area_mapping.xlsx`**

Purpose: Maps EM-DAT location names to GADM standardized names

Structure:

```
Columns:
- ISO3: GADM ISO3 code
- GADM_Country: GADM country name
- GADM_Admin1: GADM level 1 name (main)
- GADM_Admin1_Alt: Alternative names (pipe-separated)
- GADM_Admin1_Local: Local language names (pipe-separated)
- GADM_Admin2: GADM level 2 name (main)
- GADM_Admin2_Alt: Alternative names (pipe-separated)
- GADM_Admin2_Local: Local language names (pipe-separated)
```

**`IDMC_admin_area_mapping.xlsx`**

Purpose: Maps IDMC location names to GADM standardized names (same structure as EM-DAT mapping)

**`Disaster Hazard Type Map.xlsx`**

Purpose: Maps IDMC hazard classifications to EM-DAT disaster classifications

Structure:

```
Columns:
- Hazard Type: IDMC hazard type
- Hazard Sub Type: IDMC hazard subtype
- Disaster Type: Corresponding EM-DAT disaster type
- Disaster Subtype: Corresponding EM-DAT disaster subtype
```

### ISO3 Transformation Rules

Both preprocessing scripts apply these transformations to align with GADM:

| Source ISO3        | Target ISO3 | Target Country       | Special Handling                                                        |
| ------------------ | ----------- | -------------------- | ----------------------------------------------------------------------- |
| **HKG**            | CHN         | China                | Original country name added to admin_level_1 array                      |
| **MAC**            | CHN         | China                | Original country name added to admin_level_1 array                      |
| **ANT**            | NLD         | Netherlands          | Original country name added to admin_level_1 array                      |
| **AB9**            | SDN         | Sudan                | "Abyei Area" added to admin_level_2 array                               |
| **XKX** or **XKK** | XKO         | Kosovo               | Direct mapping                                                          |
| **SCG**            | SRB or MNE  | Serbia or Montenegro | Determined by matching admin locations to each country's location lists |

**Example Transformation:**

```
Original: ISO3=HKG, Country="Hong Kong"
→ Transformed: ISO3=CHN, Country="China", admin_level_1=["Hong Kong"]
```

---

## Data Preprocessing

### EM-DAT Preprocessing (`preprocess_emdat.ipynb`)

**Input:** Raw EM-DAT Excel file

**Output:** `events_emdat` table in PostgreSQL

**Processing Steps:**

#### 1. Column Selection and Filtering

Retains 30+ essential columns from the source data.

#### 2. Date Consolidation

```python
# Combines Start Year/Month/Day and End Year/Month/Day into datetime objects
# Missing month/day values default to 1
# Validates that end_date >= start_date (corrects if not)
start_date = pd.to_datetime(f"{Start Year}-{Start Month}-{Start Day}")
end_date = pd.to_datetime(f"{End Year}-{End Month}-{End Day}")
if end_date < start_date:
    end_date = start_date
```

#### 3. External ID Extraction

Parses pipe-separated `External IDs` field:

```
Format: "GLIDE:FL-2020-000012-USA|USGS:12345|DFO:4567"
→ Creates arrays: GLIDE=['FL-2020-000012-USA'], USGS=['12345'], DFO=['4567']
```

#### 4. Admin Units Extraction

Parses list of dictionaries from `Admin Units` field:

```
Format: "[{'adm1_name': 'California'}, {'adm2_name': 'Los Angeles'}]"
→ Creates arrays: admin_level_1=['California'], admin_level_2=['Los Angeles']
```

#### 5. ISO3 and Country Name Standardization

- Applies ISO3 transformation rules (see table above)
- Updates country names using `EMDAT_admin_area_mapping.xlsx`
- Validates locations against GADM names

#### 6. Admin Name Matching

Updates admin level 1 and 2 names to match GADM:

```python
def update_admin1_name(row, admin1_name):
    # Matches against GADM_Admin1, GADM_Admin1_Alt, and GADM_Admin1_Local
    matches = mapping[(mapping['ISO3'] == row['ISO3']) &
                      ((mapping['GADM_Admin1'] == admin1_name) |
                       (mapping['GADM_Admin1_Alt'].str.contains(admin1_name)) |
                       (mapping['GADM_Admin1_Local'].str.contains(admin1_name)))]
    if not matches.empty:
        return matches.iloc[0]['GADM_Admin1']
    return admin1_name  # Keep original if no match
```

#### 7. Monetary Unit Conversion

Converts from thousands of USD to USD:

```python
# Columns affected:
# - AID Contribution ('000 US$) → aid_contribution (USD)
# - Reconstruction Costs, Adjusted ('000 US$) → reconstruction_costs_adjusted (USD)
# - Total Damage, Adjusted ('000 US$) → total_damage_adjusted (USD)
value_usd = value_thousands * 1000
```

#### 8. Metadata JSON Creation

Stores non-essential columns in JSONB metadata:

```python
metadata_columns = [
    "DisNo.", "Classification Key", "Event Name", "External IDs",
    "Subregion", "Region", "Location", "Latitude", "Longitude",
    "River Basin", "Start Year", "Start Month", "Start Day",
    "End Year", "End Month", "End Day", "Admin Units"
]
metadata = json.dumps({col: row[col] for col in metadata_columns if pd.notna(row[col])})
```

#### 9. Event Name Generation

Creates unique identifiers:

```python
def create_event_name(row):
    iso3 = row['iso3_code'] if pd.notna(row['iso3_code']) else 'UNKNOWN-ISO3-CODE'
    disaster_type = row['disaster_type'] if pd.notna(row['disaster_type']) else 'UNKNOWN-DISASTER-TYPE'
    disaster_subtype = row['disaster_subtype'] if pd.notna(row['disaster_subtype']) else 'UNKNOWN-DISASTER-SUBTYPE'
    start_time = row['start_date'].strftime('%Y%m%d') if pd.notna(row['start_date']) else 'UNKNOWN-START-DATE'
    short_uuid = str(uuid.uuid4())[:8]

    return f"{iso3}_{disaster_type}_{disaster_subtype}_{start_time}_{short_uuid}"

# Example: "USA_Flood_Flash Flood_20200315_a3b5c7d9"
```

#### 10. Database Insertion

Inserts preprocessed data with conflict handling:

```sql
INSERT INTO events_emdat (...) VALUES (...)
ON CONFLICT (event_name, iso3_code, start_date) DO NOTHING
```

---

### IDMC Preprocessing (`preprocess_idmc.ipynb`)

**Input:** Raw IDMC Excel file

**Output:** `events_idmc` table in PostgreSQL

**Processing Steps:**

#### 1. Column Selection

Retains 9 essential columns from source data.

#### 2. Event Code Extraction

Parses semicolon-separated `Event Codes (Code:Type)` field:

```
Format: "FL-2020-000012:Glide Number; AP12345:IFRC Appeal ID; GOV001:Government Assigned Identifier"
→ Creates arrays:
   - GLIDE=['FL-2020-000012']
   - IFRC_Appeal_ID=['AP12345']
   - Government_Assigned_Identifier=['GOV001']
```

**Special GLIDE Processing:**
Removes ISO3 suffix from GLIDE codes:

```python
# Original: "FL-2020-000012-USA"
# Processed: "FL-2020-000012"
def process_glide_number(glide_number):
    parts = glide_number.split('-')
    return '-'.join(parts[:-1])  # Remove last part (ISO3)
```

#### 3. ISO3 Transformation

Applies same rules as EM-DAT (see ISO3 Transformation Rules table).

#### 4. Location Extraction from Event Name

IDMC event names contain location information in free-text format. Locations are extracted using regex matching against GADM location lists:

**Step 4a: Build Location Lists by ISO3**

```python
# Groups all GADM names, alternative names, and local names by ISO3
# Example for USA:
{
    'ISO3': 'USA',
    'country_list': ['United States'],
    'admin1_list': ['California', 'Texas', 'New York', ...],
    'admin2_list': ['Los Angeles', 'San Francisco', 'Houston', ...]
}
```

**Step 4b: Extract Locations from Event Name**

```python
def find_location_mentions(event_name, iso3, location_df):
    """
    Finds location mentions in event name using regex matching.

    Example Event Name: "Earthquake - California - Los Angeles 20200315"
    → Matches: [('California', 'admin1'), ('Los Angeles', 'admin2')]
    """
    # Get location lists for the ISO3 code
    country_list = location_df[location_df['ISO3'] == iso3]['country_list'].iloc[0]
    admin1_list = location_df[location_df['ISO3'] == iso3]['admin1_list'].iloc[0]
    admin2_list = location_df[location_df['ISO3'] == iso3]['admin2_list'].iloc[0]

    # Sort by length (longest first) to match longer names before shorter ones
    all_locations = sorted(
        [(loc, 'country') for loc in country_list] +
        [(loc, 'admin1') for loc in admin1_list] +
        [(loc, 'admin2') for loc in admin2_list],
        key=lambda x: len(x[0]), reverse=True
    )

    # Create regex pattern for whole-word matches
    pattern = r'\b(?:' + '|'.join(re.escape(loc[0]) for loc in all_locations) + r')\b'

    # Find matches (case-insensitive)
    matches = re.findall(pattern, event_name, re.IGNORECASE)

    return [(loc[0], loc[1]) for loc in all_locations
            if loc[0].lower() in [m.lower() for m in matches]]
```

**Step 4c: Transform to GADM Names**

```python
def transform_locations(extracted_locations, iso3, mapping_df):
    """
    Converts extracted location names to GADM standard names.

    Checks against:
    - GADM_Admin1 (main name)
    - GADM_Admin1_Alt (alternative names, pipe-separated)
    - GADM_Admin1_Local (local language names, pipe-separated)

    Same for Admin2.
    """
    result = {'admin_level_1': [], 'admin_level_2': []}

    for location, admin_level in extracted_locations:
        if admin_level == 'admin1':
            # Find matching row in mapping
            match = mapping_df[
                (mapping_df['ISO3'] == iso3) &
                ((mapping_df['GADM_Admin1'] == location) |
                 (mapping_df['GADM_Admin1_Alt'].str.contains(location)) |
                 (mapping_df['GADM_Admin1_Local'].str.contains(location)))
            ]
            if not match.empty:
                result['admin_level_1'].append(match['GADM_Admin1'].iloc[0])
        # Similar logic for admin2

    return result
```

**Example Location Extraction:**

```
Event Name: "Typhoon - Guangdong - Shenzhen 20200815"
ISO3: CHN
→ Extracted: [('Guangdong', 'admin1'), ('Shenzhen', 'admin2')]
→ Transformed: admin_level_1=['Guangdong'], admin_level_2=['Shenzhen']
```

#### 5. Hazard Type Mapping

Maps IDMC hazard classifications to EM-DAT disaster types using `Disaster Hazard Type Map.xlsx`:

```python
# Example mapping:
hazard_map = {
    ('Flood', 'Flash flood'): ('Flood', 'Flash flood'),
    ('Storm', 'Tropical cyclone'): ('Storm', 'Tropical cyclone'),
    ('Earthquake', None): ('Earthquake', 'Ground movement'),
}

# Applied to data:
disaster_type, disaster_subtype = hazard_map.get(
    (row['Hazard Type'], row['Hazard Sub Type']),
    (row['Hazard Type'], row['Hazard Sub Type'])  # Keep original if no mapping
)
```

#### 6. Metadata JSON Creation

```python
metadata_columns = [
    "Event Name", "Event Codes (Code:Type)", "Year",
    "Hazard Type", "Hazard Sub Type"
]
metadata = json.dumps({col: row[col] for col in metadata_columns if pd.notna(row[col])})
```

#### 7. Event Name Generation

Uses same pattern as EM-DAT (see EM-DAT step 9).

#### 8. Database Insertion

```sql
INSERT INTO events_idmc (...) VALUES (...)
ON CONFLICT (event_name, iso3_code, start_date) DO NOTHING
```

---

## Database Schema

### Table: `events_emdat`

Stores preprocessed EM-DAT data.

```sql
CREATE TABLE IF NOT EXISTS events_emdat (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    disaster_group VARCHAR(50),              -- Natural, Technological, Complex
    disaster_subgroup VARCHAR(50),           -- Climatological, Geophysical, etc.
    disaster_type VARCHAR(50),               -- Flood, Earthquake, Storm, etc.
    disaster_subtype VARCHAR(50),            -- Flash flood, Ground movement, etc.
    iso3_code CHAR(3),                       -- GADM-standardized ISO3
    admin_level_0 VARCHAR(100),              -- Country name (GADM standard)
    admin_level_1 TEXT[],                    -- Array of admin1 names
    admin_level_2 TEXT[],                    -- Array of admin2 names
    start_date DATE,
    end_date DATE,
    total_deaths INTEGER,
    number_injured INTEGER,
    number_affected INTEGER,
    number_homeless INTEGER,
    total_affected INTEGER,
    total_damage_adjusted FLOAT,            -- In USD (not thousands)
    reconstruction_costs_adjusted FLOAT,    -- In USD (not thousands)
    aid_contribution FLOAT,                 -- In USD (not thousands)
    disaster_internal_displacements INTEGER,
    source VARCHAR(50),                     -- Always 'EMDAT'
    metadata JSONB,                         -- Original fields (DisNo, coordinates, etc.)
    USGS TEXT[],                            -- Array of USGS identifiers
    GLIDE TEXT[],                           -- Array of GLIDE codes
    DFO TEXT[]                              -- Array of DFO identifiers
);

-- Unique constraint prevents duplicate entries
ALTER TABLE events_emdat
ADD CONSTRAINT unique_emdat_entry UNIQUE (event_name, iso3_code, start_date);
```

**Key Points:**

- **admin_level_1/2**: Arrays allow multiple locations per event
- **Monetary values**: Stored in full USD (converted from thousands)
- **metadata**: JSONB column stores original source attributes
- **Unique constraint**: Based on event name, country, and start date

---

### Table: `events_idmc`

Stores preprocessed IDMC data.

```sql
CREATE TABLE IF NOT EXISTS events_idmc (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    disaster_type VARCHAR(50),               -- Mapped from hazard type
    disaster_subtype VARCHAR(50),            -- Mapped from hazard subtype
    iso3_code CHAR(3),                       -- GADM-standardized ISO3
    admin_level_0 VARCHAR(100),              -- Country name (GADM standard)
    admin_level_1 TEXT[],                    -- Extracted and matched locations
    admin_level_2 TEXT[],                    -- Extracted and matched locations
    start_date DATE,
    disaster_internal_displacements INTEGER, -- Primary metric from IDMC
    source VARCHAR(50),                      -- Always 'IDMC'
    metadata JSONB,                          -- Original fields (Event Name, Year, etc.)
    GLIDE TEXT[],                            -- Array of GLIDE codes (ISO3 suffix removed)
    local_Identifier TEXT[],                 -- Array of local identifiers
    IFRC_Appeal_ID TEXT[],                   -- Array of IFRC appeal IDs
    Government_Assigned_Identifier TEXT[]    -- Array of government identifiers
);

-- Unique constraint prevents duplicate entries
ALTER TABLE events_idmc
ADD CONSTRAINT unique_event_idmc_entry UNIQUE (event_name, iso3_code, start_date);
```

**Key Points:**

- **disaster_type/subtype**: Mapped from IDMC hazard classifications
- **admin_level_1/2**: May be empty if no locations extracted from event name
- **GLIDE**: Processed to remove ISO3 suffix for matching
- **Unique constraint**: Based on event name, country, and start date

---

### Table: `events` (Unified/Merged)

Combines events from both sources using GLIDE code matching.

```sql
CREATE TABLE IF NOT EXISTS events (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    disaster_group VARCHAR(50),
    disaster_subgroup VARCHAR(50),
    disaster_type VARCHAR(50),
    disaster_subtype VARCHAR(50),
    iso3_code CHAR(3),
    admin_level_0 VARCHAR(100),
    admin_level_1 TEXT[],
    admin_level_2 TEXT[],
    start_date DATE,
    end_date DATE,
    total_deaths INTEGER,
    number_injured INTEGER,
    number_affected INTEGER,
    number_homeless INTEGER,
    total_affected INTEGER,
    total_damage_adjusted FLOAT,
    reconstruction_costs_adjusted FLOAT,
    aid_contribution FLOAT,
    disaster_internal_displacements INTEGER, -- From EM-DAT or IDMC (merged)
    source VARCHAR(50),                      -- 'EMDAT', 'IDMC', or both
    metadata JSONB,
    USGS TEXT[],
    GLIDE TEXT[],
    DFO TEXT[],
    local_Identifier TEXT[],
    IFRC_Appeal_ID TEXT[],
    Government_Assigned_Identifier TEXT[]
);
```

**Merging Logic:**

- All EM-DAT events inserted first
- IDMC events matched by GLIDE code:
  - If GLIDE match found: Update `disaster_internal_displacements` in existing EM-DAT event
  - If no GLIDE match: Insert as new event
- Source column indicates data origin

---

## Data Merging

### Merge Process (`merge_events_with_seperate_events_table.ipynb`)

**Purpose:** Combines EM-DAT and IDMC data into a unified `events` table using GLIDE code matching.

**Steps:**

#### 1. Create Unified Events Table

```python
create_events_table(cur)  # Creates events table with all columns from both sources
```

#### 2. Insert All EM-DAT Events

```sql
INSERT INTO events (
    event_name, disaster_group, disaster_subgroup, disaster_type, disaster_subtype,
    iso3_code, admin_level_0, admin_level_1, admin_level_2, start_date, end_date,
    total_deaths, number_injured, number_affected, number_homeless, total_affected,
    total_damage_adjusted, reconstruction_costs_adjusted, aid_contribution,
    disaster_internal_displacements, source, metadata, USGS, GLIDE, DFO
)
SELECT * FROM events_emdat;
```

#### 3. Update or Insert IDMC Events

```sql
-- Update strategy using GLIDE array overlap
WITH idmc_data AS (
    SELECT * FROM events_idmc
),
updated AS (
    -- Update existing events where GLIDE codes overlap
    UPDATE events e
    SET disaster_internal_displacements = i.disaster_internal_displacements
    FROM idmc_data i
    WHERE EXISTS (
        SELECT 1
        FROM unnest(e.GLIDE) e_glide
        JOIN unnest(i.GLIDE) i_glide ON e_glide = i_glide
    )
    RETURNING e.*
)
-- Insert IDMC events that didn't match any GLIDE codes
INSERT INTO events (
    event_name, disaster_type, disaster_subtype, iso3_code, admin_level_0,
    admin_level_1, admin_level_2, start_date, disaster_internal_displacements,
    source, metadata, GLIDE, local_Identifier, IFRC_Appeal_ID, Government_Assigned_Identifier
)
SELECT
    i.event_name, i.disaster_type, i.disaster_subtype, i.iso3_code, i.admin_level_0,
    i.admin_level_1, i.admin_level_2, i.start_date, i.disaster_internal_displacements,
    i.source, i.metadata, i.GLIDE, i.local_Identifier, i.IFRC_Appeal_ID, i.Government_Assigned_Identifier
FROM idmc_data i
WHERE NOT EXISTS (
    SELECT 1 FROM updated u
    WHERE EXISTS (
        SELECT 1
        FROM unnest(u.GLIDE) u_glide
        JOIN unnest(i.GLIDE) i_glide ON u_glide = i_glide
    )
);
```

**Merge Outcome:**

- Events with matching GLIDE codes: EM-DAT data enriched with IDMC displacement figures
- Events unique to EM-DAT: Retained as-is
- Events unique to IDMC: Inserted as new records

---

## Views and Indexes

### View: `events_flattened_admin`

**Purpose:** Flattens admin area arrays into individual rows for easier querying.

```sql
CREATE TABLE events_flattened_admin AS
-- Admin level 0 (country)
SELECT
    e.*,
    0 AS admin_level,
    e.admin_level_0 AS admin_name
FROM events e

UNION ALL

-- Admin level 1 (state/province)
SELECT
    e.*,
    1 AS admin_level,
    unnest(e.admin_level_1) AS admin_name
FROM events e
WHERE array_length(e.admin_level_1, 1) > 0

UNION ALL

-- Admin level 2 (county/district)
SELECT
    e.*,
    2 AS admin_level,
    unnest(e.admin_level_2) AS admin_name
FROM events e
WHERE array_length(e.admin_level_2, 1) > 0;
```

**Example:**

```
Original row:
  event_id=1, admin_level_0='USA', admin_level_1=['California', 'Nevada'], admin_level_2=[]

Flattened rows:
  event_id=1, admin_level=0, admin_name='USA'
  event_id=1, admin_level=1, admin_name='California'
  event_id=1, admin_level=1, admin_name='Nevada'
```

**Indexes:**

```sql
CREATE INDEX idx_flattened_events_event_id ON events_flattened_admin (event_id);
CREATE INDEX idx_flattened_events_admin_level ON events_flattened_admin (admin_level);
CREATE INDEX idx_flattened_events_admin_name ON events_flattened_admin (admin_name);
CREATE INDEX idx_flattened_events_iso3_code ON events_flattened_admin (iso3_code);
```

---

### View: `events_with_geometry`

**Purpose:** Joins event data with GADM geometries for spatial analysis.

```sql
CREATE OR REPLACE VIEW events_with_geometry AS
SELECT
    fe.event_id,
    fe.event_name,
    fe.disaster_group,
    fe.disaster_subgroup,
    fe.disaster_type,
    fe.disaster_subtype,
    fe.iso3_code,
    fe.admin_level,
    fe.admin_name,
    fe.start_date,
    fe.end_date,
    fe.total_deaths,
    fe.number_injured,
    fe.number_affected,
    fe.number_homeless,
    fe.total_affected,
    fe.total_damage_adjusted,
    fe.reconstruction_costs_adjusted,
    fe.aid_contribution,
    fe.disaster_internal_displacements,
    fe.source,
    fe.metadata,
    fe.USGS,
    fe.GLIDE,
    fe.DFO,
    fe.local_Identifier,
    fe.IFRC_Appeal_ID,
    fe.Government_Assigned_Identifier,
    gc.id AS gid,                  -- GADM geometry ID
    gc.centroid_lat,
    gc.centroid_long,
    gc.geojson_polygon
FROM
    events_flattened_admin fe
LEFT JOIN
    gadm_combined gc ON
    CASE
        WHEN fe.admin_level = 0 THEN gc.admin_level = 'admin0'
        WHEN fe.admin_level = 1 THEN gc.admin_level = 'admin1'
        WHEN fe.admin_level = 2 THEN gc.admin_level = 'admin2'
    END
    AND fe.admin_name = gc.admin_name
    AND (fe.iso3_code = gc.ISO3 OR fe.iso3_code IS NULL)
WHERE
    gc.id IS NOT NULL;
```

**Use Cases:**

- Spatial visualization of disaster events
- Geographic clustering analysis
- Distance calculations between events
- Overlaying with other geospatial datasets

---

### Main Table Indexes

**`events` table:**

```sql
-- Scalar column indexes
CREATE INDEX IF NOT EXISTS idx_disaster_type ON events (disaster_type);
CREATE INDEX IF NOT EXISTS idx_disaster_subtype ON events (disaster_subtype);
CREATE INDEX IF NOT EXISTS idx_iso3_code ON events (iso3_code);
CREATE INDEX IF NOT EXISTS idx_admin_level_0 ON events (admin_level_0);
CREATE INDEX IF NOT EXISTS idx_start_date ON events (start_date);
CREATE INDEX IF NOT EXISTS idx_glide ON events (GLIDE);

-- GIN indexes for array columns (enables array containment queries)
CREATE INDEX IF NOT EXISTS idx_admin_level_1 ON events USING GIN (admin_level_1);
CREATE INDEX IF NOT EXISTS idx_admin_level_2 ON events USING GIN (admin_level_2);
```

**Query Performance:**

- Scalar indexes support fast filtering by disaster type, country, date
- GIN indexes enable efficient array searches:
  ```sql
  -- Find events affecting California
  SELECT * FROM events WHERE 'California' = ANY(admin_level_1);
  ```

---

## Usage Instructions

### Prerequisites

1. **PostgreSQL with PostGIS** installed and running
2. **GADM administrative boundaries** imported (see [GADM/README.md](../GADM/README.md))
3. **Python environment** with required packages:

   ```bash
   pip install pandas psycopg2-binary openpyxl
   ```

4. **Data files:**
   - Raw EM-DAT Excel file
   - Raw IDMC Excel file
   - `EMDAT_admin_area_mapping.xlsx` (in Events/EM-DAT/)
   - `IDMC_admin_area_mapping.xlsx` (in Events/IDMC/)
   - `Disaster Hazard Type Map.xlsx` (in Events/IDMC/)

---

### Execution Workflow

**IMPORTANT:** Follow this exact sequence to ensure proper data integration.

#### Step 1: Create Database Tables

```bash
# Create EM-DAT table
cd Events/EM-DAT/
python create_table_emdat.py
# Enter DB password and host when prompted

# Create IDMC table
cd ../IDMC/
python create_table_idmc.py
# Enter DB password and host when prompted
```

**Verification:**

```sql
-- Check tables exist
\dt events_emdat
\dt events_idmc

-- Verify schemas
\d events_emdat
\d events_idmc
```

---

#### Step 2: Preprocess and Load EM-DAT Data

```bash
cd Events/EM-DAT/
jupyter notebook preprocess_emdat.ipynb
```

**In the notebook:**

1. **Cell 1**: Enter path to raw EM-DAT Excel file when prompted
2. **Execute cells sequentially** (cells 0-23)
3. **Cell 23**: Enter DB password and host when prompted for data insertion
4. **Final cell**: Review data summary statistics

**Expected Output:**

```
Data inserted successfully into events_emdat table.
EM-DAT data insertion process completed.
Inserted X rows for events_emdat
```

**Verification:**

```sql
SELECT COUNT(*) FROM events_emdat;
SELECT COUNT(*) FROM events_emdat WHERE admin_level_1 IS NOT NULL;
SELECT COUNT(*) FROM events_emdat WHERE GLIDE IS NOT NULL;
```

---

#### Step 3: Preprocess and Load IDMC Data

```bash
cd Events/IDMC/
jupyter notebook preprocess_idmc.ipynb
```

**In the notebook:**

1. **Cell 1**: Enter path to raw IDMC Excel file when prompted
2. **Execute cells sequentially** (cells 0-36)
3. **Cell 35**: Enter DB password and host when prompted for data insertion
4. **Final cell**: Review data summary statistics

**Expected Output:**

```
Data inserted successfully into events_idmc table.
IDMC data insertion process completed.
Inserted X rows for events_idmc
```

**Verification:**

```sql
SELECT COUNT(*) FROM events_idmc;
SELECT COUNT(*) FROM events_idmc WHERE admin_level_1 != '{}';
SELECT COUNT(*) FROM events_idmc WHERE GLIDE IS NOT NULL;
```

---

#### Step 4: Merge Events from Both Sources

```bash
cd Events/
jupyter notebook merge_events_with_seperate_events_table.ipynb
```

**In the notebook:**

1. **Cell 1**: Enter DB password and host when prompted
2. **Cell 3**: Execute merge logic
3. **Review output** to confirm merge statistics

**Expected Output:**

```
Data merged, inserted, and indexed successfully.
PostgreSQL connection is closed
```

**Verification:**

```sql
-- Check total event count
SELECT COUNT(*) FROM events;

-- Count by source
SELECT source, COUNT(*) FROM events GROUP BY source;

-- Verify GLIDE matches
SELECT COUNT(*) FROM events
WHERE disaster_internal_displacements IS NOT NULL
  AND source = 'EMDAT';  -- These are enriched events
```

---

#### Step 5: Create Views and Indexes

Execute the SQL commands from the notebook cells manually or via the notebook:

**Create indexes (cell 4):**

```sql
CREATE INDEX IF NOT EXISTS idx_disaster_type ON events (disaster_type);
CREATE INDEX IF NOT EXISTS idx_disaster_subtype ON events (disaster_subtype);
CREATE INDEX IF NOT EXISTS idx_iso3_code ON events (iso3_code);
CREATE INDEX IF NOT EXISTS idx_admin_level_0 ON events (admin_level_0);
CREATE INDEX IF NOT EXISTS idx_start_date ON events (start_date);
CREATE INDEX IF NOT EXISTS idx_glide ON events (GLIDE);
CREATE INDEX IF NOT EXISTS idx_admin_level_1 ON events USING GIN (admin_level_1);
CREATE INDEX IF NOT EXISTS idx_admin_level_2 ON events USING GIN (admin_level_2);
```

**Create flattened view (cell 5):**

```sql
CREATE TABLE events_flattened_admin AS
SELECT e.*, 0 AS admin_level, e.admin_level_0 AS admin_name
FROM events e
UNION ALL
SELECT e.*, 1 AS admin_level, unnest(e.admin_level_1) AS admin_name
FROM events e WHERE array_length(e.admin_level_1, 1) > 0
UNION ALL
SELECT e.*, 2 AS admin_level, unnest(e.admin_level_2) AS admin_name
FROM events e WHERE array_length(e.admin_level_2, 1) > 0;

CREATE INDEX idx_flattened_events_event_id ON events_flattened_admin (event_id);
CREATE INDEX idx_flattened_events_admin_level ON events_flattened_admin (admin_level);
CREATE INDEX idx_flattened_events_admin_name ON events_flattened_admin (admin_name);
CREATE INDEX idx_flattened_events_iso3_code ON events_flattened_admin (iso3_code);
```

**Create geometry view (cell 6):**

```sql
CREATE OR REPLACE VIEW events_with_geometry AS
SELECT
    fe.*,
    gc.id AS gid,
    gc.centroid_lat,
    gc.centroid_long,
    gc.geojson_polygon
FROM events_flattened_admin fe
LEFT JOIN gadm_combined gc ON
    CASE
        WHEN fe.admin_level = 0 THEN gc.admin_level = 'admin0'
        WHEN fe.admin_level = 1 THEN gc.admin_level = 'admin1'
        WHEN fe.admin_level = 2 THEN gc.admin_level = 'admin2'
    END
    AND fe.admin_name = gc.admin_name
    AND (fe.iso3_code = gc.ISO3 OR fe.iso3_code IS NULL)
WHERE gc.id IS NOT NULL;
```

**Verification:**

```sql
-- Check view creation
\dv events_with_geometry

-- Count flattened rows
SELECT COUNT(*) FROM events_flattened_admin;

-- Count events with geometries
SELECT COUNT(DISTINCT event_id) FROM events_with_geometry;
```
