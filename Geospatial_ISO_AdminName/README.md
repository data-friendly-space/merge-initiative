# Geospatial ISO/AdminName Processing

This directory contains ETL scripts for processing datasets where data is already associated with **administrative names** or **ISO country codes**. Unlike raster-based processing, these scripts use **fuzzy name matching** to link location names in source data to GADM administrative boundaries.

> **See Also:** For a detailed summary of all variable transformations with before/after examples, see [GEOSPATIAL_ISO_TRANSFORMATION_SUMMARY.md](GEOSPATIAL_ISO_TRANSFORMATION_SUMMARY.md).

---

## Table of Contents

1. [Overview](#overview)
2. [Methodology](#methodology)
3. [Data Sources](#data-sources)
4. [Database Setup](#database-setup)
5. [Usage](#usage)
6. [Technical Implementation Details](#technical-implementation-details)
7. [Quality Control](#quality-control)
8. [Comparison: ISO/AdminName vs. Lat/Long](#comparison-isoadminname-vs-latlong)

---

## Overview

### What This Does

These scripts convert **name-based location data** into **GADM-linked records** suitable for integration with the spatial database:

```
Input:  CSV/Excel with location names (e.g., "California", "Texas", "New York")
Output: Database records linked to GADM geometries via GID
        → Stored in PostgreSQL with GID, date, variable, raw_value
```

### Key Features

- **Fuzzy name matching** using normalized string comparison
- **Multi-source name matching** - checks main names, variant names, and native language names
- **ISO3 verification** - ensures location names match within the correct country
- **Unmatched location tracking** - outputs CSV files for manual review
- **Metadata preservation** - stores original source attributes in JSONB columns
- **Flexible variable handling** - supports 40+ variables per data source
- **Idempotent inserts** - ON CONFLICT clauses allow safe re-runs

---

## Methodology

### Name Normalization

All location names undergo standardization before matching:

```python
def normalize_name(name):
    # Convert to lowercase
    normalized = name.lower()
    # Remove special characters
    normalized = re.sub(r'[^a-z0-9\s]', '', normalized)
    # Replace spaces with underscores
    normalized = normalized.replace(' ', '_')
    return normalized
```

**Examples:**

```
"São Paulo"          → "sao_paulo"
"New York"           → "new_york"
"Île-de-France"      → "iledefrance"
"北京市" (Beijing)    → "" (non-Latin characters removed)
```

**Limitation:** Non-Latin scripts are stripped, requiring native language name columns in GADM.

---

### Location Extraction

Many datasets provide **aggregate regions** rather than individual administrative units. The `extract_locations()` function parses these:

**Patterns Handled:**

1. **Parenthetical lists:**

   ```
   Input:  "Central (Kabul Wardak Kapisa Logar Parwan Panjsher)"
   Output: ["Kabul", "Wardak", "Kapisa", "Logar", "Parwan", "Panjsher"]
   ```

2. **Comma-separated lists:**

   ```
   Input:  "California, Oregon, Washington"
   Output: ["California", "Oregon", "Washington"]
   ```

3. **"and" connectors:**
   ```
   Input:  "Texas and Oklahoma"
   Output: ["Texas", "Oklahoma"]
   ```

**Filtering:**

- Removes directional terms: "North", "South", "Eastern", "Central"
- Removes aggregation terms: "Total", "Urban", "Rural", "Poor", "Nonpoor"
- Removes Roman numerals: "I-", "II-", "III-"
- Removes percentile indicators: "Lowest 25%", "Highest 25%"

---

### Matching Algorithm

The matching process searches GADM administrative boundaries in a hierarchical manner:

```python
# Step 1: Load GADM name dictionaries (done once at script startup)
unique_admin1_names = get_unique_admin_names(
    'gadm_admin1_new',
    main_column='admin_level_1',
    var_name_column='admin_level_1_var_name',
    nl_name_column='admin_level_1_nl_name',
    gid_column='gid_1',
    iso_column='iso3'
)

unique_admin2_names = get_unique_admin_names(...)

# Step 2: For each location in source data
normalized_location = normalize_name(location)

# Step 3: Check level 1 first, then level 2
if normalized_location in unique_admin1_names:
    gid, iso = unique_admin1_names[normalized_location]
    if iso == row['iso_code']:  # Verify country matches
        admin_level = 1
        use_gid = gid

elif normalized_location in unique_admin2_names:
    gid, iso = unique_admin2_names[normalized_location]
    if iso == row['iso_code']:
        admin_level = 2
        use_gid = gid

else:
    # No match found - add to unmatched_locations.csv
```

**Why ISO3 verification matters:**

Without ISO verification, "Georgia" could match:

- Georgia (U.S. state) - USA.12_1
- Georgia (country) - GEO_1

ISO3 check ensures we match "Georgia" to USA.12_1 only when `iso_code = 'USA'`.

---

### Multi-Source Matching

GADM provides three name sources for each administrative unit:

| Column                   | Example                         | Description                   |
| ------------------------ | ------------------------------- | ----------------------------- |
| `admin_level_1`          | "California"                    | Official English name         |
| `admin_level_1_var_name` | ["CA", "Calif."]                | Common abbreviations/variants |
| `admin_level_1_nl_name`  | ["California", "कैलिफ़ोर्निया"] | Native language names         |

The matching function queries **all three sources**:

```python
# Query main names
cur.execute("SELECT DISTINCT admin_level_1, gid_1, iso3 FROM gadm_admin1_new;")
unique_names.update({normalize_name(row[0]): (row[1], row[2])})

# Query variant names (array column)
cur.execute("SELECT DISTINCT unnest(admin_level_1_var_name), gid_1, iso3 FROM gadm_admin1_new;")
unique_names.update({normalize_name(row[0]): (row[1], row[2])})

# Query native language names (array column)
cur.execute("SELECT DISTINCT unnest(admin_level_1_nl_name), gid_1, iso3 FROM gadm_admin1_new;")
unique_names.update({normalize_name(row[0]): (row[1], row[2])})
```

**Benefit:** Maximizes match rate by checking aliases, abbreviations, and local spellings.

---

### Unmatched Location Handling

When a location cannot be matched to GADM, it's recorded in a CSV file for manual review:

**File naming pattern:** `unmatched_locations_{dataset}.csv`

**Example:** `unmatched_locations_Area.csv`

```csv
ISO Code,Location,Original Region
AFG,Kabul,Central (Kabul Wardak Kapisa Logar Parwan Panjsher)
BGD,Dhaka,Dhaka Division
IND,Mumbai,Maharashtra (Mumbai Pune Nagpur)
```

**Columns:**

- `ISO Code`: Country identifier (for filtering)
- `Location`: Extracted location name (as seen in source data)
- `Original Region`: Full region string from source data (for context)

**Workflow:**

1. Run ETL script
2. Review `unmatched_locations_*.csv`
3. Options:
   - Add missing names to GADM variant names
   - Manually map in source data
   - Report to GADM for future inclusion
4. Re-run script after corrections

---

## Data Sources

### GDL (Global Data Lab)

The GDL provides **subnational development indicators** at state/province and district levels for 150+ countries.

#### GDL Area (42 variables)

**Script:** `GDL/Area/GDL_Area_ETL.py`

**Source format:** CSV files with "Area" in filename

**Variables:**

- **Wealth:** `iwi` (International Wealth Index), `iwipov70`, `iwipov50`, `iwipov35` (poverty rates)
- **Technology:** `internet`, `cellphone` (access rates)
- **Inequality:** `thtwithin`, `thtbetween` (Theil indices)
- **Urbanization:** `urban` (percentage)
- **Education:** `edyr25` (years of education), `womedyr25`, `menedyr25` (sex-disaggregated)
- **Labor:** `workwom` (women in workforce), `wagri`, `wwrklow`, `wwrkhigh`, `hagri`, `hwrklow`, `hwrkhigh`
- **Demographics:** `agedifmar`, `agemarw20` (marriage indicators), `tfr` (total fertility rate)
- **Health:** `stunting`, `haz`, `whz`, `waz`, `bmiz` (child nutrition indices), `dtp3age1`, `measlage1` (vaccination)
- **Population:** `regpopm` (population in millions), `popshare`, age groups (`age09` through `age90hi`), `hhsize`, `popworkage`, `popold`
- **Mortality:** `infmort`, `u5mort`
- **Infrastructure:** `pipedwater`, `electr`

**Special handling:**

- `regpopm` standardized from millions to persons (×1,000,000)
- National-level data uses `ISO3` directly (admin_level = 0)
- Subnational data uses fuzzy matching (admin_level = 1 or 2)

**Output:** `unmatched_locations_Area.csv`

---

#### GDL SHDI/SGDI (16 variables)

**Script:** `GDL/SHDI_SGDI/GDL_SHDI_SGDI_ETL.py`

**Source format:** CSV files with "SHDI" in filename

**Variables:**

- **Composite indices:** `shdi` (Subnational HDI), `sgdi` (Subnational Gender Development Index)
- **HDI components:** `healthindex`, `edindex`, `incindex`
- **Sex-disaggregated indices:** `shdif`, `shdim`, `healthindexf`, `healthindexm`, `edindexf`, `edindexm`
- **Life expectancy:** `lifexp`, `lifexpf`, `lifexpm`
- **Income:** `lgnic` (log gross national income per capita)
- **Population:** `pop` (in thousands)

**Special handling:**

- `pop` standardized from thousands to persons (×1,000)
- SHDI/SGDI values range from 0-1 (higher = more developed)

**Output:** `unmatched_locations_SHDI_SGDI.csv`

---

#### GDL Geospatial (3 variables)

**Script:** `GDL/Geospatial/GDL_Geospatial_ETL.py`

**Source format:** CSV files with "weather" in filename

**Variables:**

- `surfacetempyear`: Annual mean surface temperature (°C)
- `relhumidityyear`: Annual mean relative humidity (%)
- `totprecipyear`: Total annual precipitation (mm)

**Note:** These are pre-aggregated climate statistics, not raw gridded data.

**Output:** `unmatched_locations_weather.csv`

---

### WorldPop-PWD (Population Weighted Density)

WorldPop provides **population-weighted centroid density** calculations at national and subnational levels.

#### National Level

**Script:** `WorldPop-PWD/WorldPop_PWD_national_ETL.py`

**Source format:** GeoJSON files

**Process:**

1. Reads all `.geojson` files in folder
2. Extracts `ISO` field directly (no fuzzy matching needed)
3. Sets `admin_level = 0`
4. Inserts `Population_Weighted_Density_G` variable

**Key fields:**

- `PWD_G`: Population-weighted density (persons/km²)
- `Pop`: Total population
- `Density`: Arithmetic density
- `Area`: Land area (km²)
- `PWC_Lat`, `PWC_Lon`: Population-weighted centroid coordinates

**Metadata preserved:** All geometric and demographic attributes stored in JSONB

---

#### Sub-national Level

**Script:** `WorldPop-PWD/WorldPop_PWD_sub_national_ETL.py`

**Source format:** GeoJSON files

**Process:**

1. Reads all `.geojson` files in folder
2. Extracts `GID_1` field directly from WorldPop (WorldPop uses GADM GIDs)
3. Sets `admin_level = 1`
4. Inserts `Population_Weighted_Density_G` variable

**Key difference from National:**

- Uses `GID_1` instead of `ISO`
- Includes `Adm_N` (administrative unit name)
- Represents state/province level data

**Note:** WorldPop GIDs are GADM-compliant, so **no fuzzy matching is required**.

---

### IDMC (Internal Displacement Monitoring Centre)

IDMC provides **displacement statistics** by country, including conflict-related and disaster-related displacement.

#### Country Displacement

**Script:** `IDMC/Country_Displacement/IDMC_Country_Displacement_ETL.py`

**Source format:** Excel files with sheet `1_Displacement_data`

**Variables (original names):**

- `Conflict Stock Displacement`: Total IDPs from conflict
- `Conflict Internal Displacements`: New conflict displacements during year
- `Disaster Internal Displacements`: New disaster displacements during year
- `Disaster Stock Displacement`: Total IDPs from disasters

**Processing:**

1. Reads all Excel files in folder
2. Removes duplicates by (`ISO3`, `Name`, `Year`, `source_file`)
3. Groups by (`ISO3`, `Name`, `Year`, `source_file`) and sums variables
4. Renames variables: "Stock" → "Total"
5. Maps ISO3 codes: `XKX`→`XKO`, `AB9`→`SDN`, `HKG`→`CHN`, `MAC`→`CHN`
6. Sets `admin_level = 0` (national level)

**Output variables (renamed):**

- `Conflict Total Displacement`
- `Conflict Internal Displacements`
- `Disaster Internal Displacements`
- `Disaster Total Displacement`

---

#### IDPs SADD Estimates (Sex and Age Disaggregated Data)

**Script:** `IDMC/IDPs_SADD_estimates/IDMC_IDPs_SADD_estimates_ETL.py`

**Source format:** Excel files with sheet `3_IDPs_SADD_estimates`

**Age groups (variables):**

- `0-4`: Children under 5
- `5-11`: School-age children
- `12-17`: Adolescents
- `18-59`: Working-age adults
- `60+`: Elderly

**Processing:**

1. Reads all Excel files in folder
2. Removes duplicates by (`ISO3`, `Year`, `Sex`, `Cause`)
3. Groups by (`ISO3`, `Country`, `Year`, `Sex`, `Cause`, `source_file`) and sums age groups
4. Creates variable names combining `Cause`, `Sex`, and age group
5. Maps ISO3 codes (same as Country Displacement)
6. Sets `admin_level = 0`

**Output variable naming pattern:**

```
{Cause}_{Sex}_{AgeGroup}

Examples:
- Conflict_Male_0-4
- Conflict_Female_18-59
- Disaster_Male_60+
- Disaster_Female_5-11
```

**Causes:**

- `Conflict`: Displaced due to violence, persecution, or war
- `Disaster`: Displaced due to natural disasters (floods, storms, earthquakes, droughts)

**Sex:**

- `Male`
- `Female`

---

## Database Setup

Before running ETL scripts, create the necessary database tables using the provided `create_table_*.py` scripts.

### Available Scripts

```
Geospatial_ISO_AdminName/
├── GDL/create_table_gdl.py
├── IDMC/create_table_idmc.py
└── WorldPop-PWD/create_table_worldpop_pwd.py
```

### Table Schemas

#### GDL Table

**Table name:** `geospatial_data_gdl`

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_gdl (
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
    metadata JSONB,
    UNIQUE (gid, admin_level, date, variable)
);

CREATE INDEX idx_gdl_gid ON geospatial_data_gdl(gid);
CREATE INDEX idx_gdl_date ON geospatial_data_gdl(date);
CREATE INDEX idx_gdl_variable ON geospatial_data_gdl(variable);
CREATE INDEX idx_gdl_metadata ON geospatial_data_gdl USING GIN(metadata);
```

**Notes:**

- `raw_value` is the primary data field (others NULL for this source)
- `note` contains "Extracted from: {original_region}" when locations were split
- `source` contains filename for traceability
- `metadata` stores original CSV columns (ISO code, year, GDLCODE, etc.)

---

#### IDMC Table

**Table name:** `geospatial_data_idmc`

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_idmc (
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
    metadata JSONB,
    UNIQUE (gid, admin_level, date, variable)
);

CREATE INDEX idx_idmc_gid ON geospatial_data_idmc(gid);
CREATE INDEX idx_idmc_date ON geospatial_data_idmc(date);
CREATE INDEX idx_idmc_variable ON geospatial_data_idmc(variable);
CREATE INDEX idx_idmc_metadata ON geospatial_data_idmc USING GIN(metadata);
```

**Notes:**

- `gid` is ISO3 code (since all IDMC data is national level)
- `admin_level` is always 0
- `variable` includes cause and sex for SADD data (e.g., "Conflict_Male_0-4")
- `metadata` stores ISO3, Country, Year, Sex, Cause

---

#### WorldPop-PWD Table

**Table name:** `geospatial_data_worldpop_pwd`

```sql
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
    metadata JSONB,
    UNIQUE (gid, admin_level, date, variable)
);

CREATE INDEX idx_worldpop_pwd_gid ON geospatial_data_worldpop_pwd(gid);
CREATE INDEX idx_worldpop_pwd_date ON geospatial_data_worldpop_pwd(date);
CREATE INDEX idx_worldpop_pwd_variable ON geospatial_data_worldpop_pwd(variable);
CREATE INDEX idx_worldpop_pwd_metadata ON geospatial_data_worldpop_pwd USING GIN(metadata);
```

**Notes:**

- `gid` is ISO3 for national (admin_level = 0) or GID_1 for subnational (admin_level = 1)
- `raw_value` contains `PWD_G` (population-weighted density)
- `note` is "GID directly from WorldPop" (no fuzzy matching)
- `metadata` stores full GeoJSON attributes (Lon, Lat, Pop, Density, Area, PWC coordinates)

---

### Running Create Table Scripts

```bash
cd Geospatial_ISO_AdminName/GDL/
python create_table_gdl.py

# Interactive prompts:
Enter the database password: ****
Enter the database host: localhost
```

**Best practices:**

1. Create tables BEFORE running ETL scripts
2. Scripts use `CREATE TABLE IF NOT EXISTS` - safe to re-run
3. Unique constraints prevent duplicate entries
4. JSONB indexes enable fast metadata queries

---

## Usage

### Prerequisites

```bash
# Install dependencies
pip install pandas psycopg2 numpy openpyxl geopandas

# Set PROJECT_ROOT environment variable (for GDL and IDMC scripts)
export PROJECT_ROOT="/path/to/merge-initiative"

# Update config.json with folder paths
{
  "GDL_FOLDER": "/path/to/gdl/csv/files",
  "IDMC_FOLDER": "/path/to/idmc/excel/files",
  "LOCAL_DB_CONFIG": {
    "dbname": "merge",
    "user": "postgres",
    "password": "****",
    "host": "localhost",
    "port": "5432"
  }
}
```

---

### Running ETL Scripts

#### GDL Pattern

**Using config.json:**

```bash
cd Geospatial_ISO_AdminName/GDL/Area/
python GDL_Area_ETL.py

# Output:
Number of GDL files read: 5
Number of rows after processing: 12345
Inserted 49380 rows into geospatial_data_gdl table.
```

**If unmatched locations exist:**

```
unmatched_locations_Area.csv created with 23 entries
```

Review the CSV file and update GADM variant names or source data as needed.

---

#### WorldPop-PWD Pattern

**Interactive prompts:**

```bash
cd Geospatial_ISO_AdminName/WorldPop-PWD/
python WorldPop_PWD_national_ETL.py

# Prompts:
Enter the path to the folder containing GeoJSON files: /path/to/geojson
Enter the database password: ****
Enter the database host: localhost

# Output:
Inserted chunk of 195 rows.
Inserted 195 rows into the database.
```

**Sub-national follows same pattern:**

```bash
python WorldPop_PWD_sub_national_ETL.py
# Same prompts, different admin_level and GID field
```

---

#### IDMC Pattern

**Using config.json:**

```bash
cd Geospatial_ISO_AdminName/IDMC/Country_Displacement/
python IDMC_Country_Displacement_ETL.py

# Output:
Number of IDMC files read: 3
Number of rows after processing: 1234
Inserted 4936 rows into geospatial_data_idmc table.
```

**SADD estimates follow same pattern:**

```bash
cd ../IDPs_SADD_estimates/
python IDMC_IDPs_SADD_estimates_ETL.py

# Output:
Number of IDMC files read: 3
Number of rows after processing: 2468
Inserted 61700 rows into geospatial_data_idmc table.
```

---

### Processing Flow

**All scripts follow this general flow:**

1. **Extract:**

   - Read CSV/Excel/GeoJSON files from folder
   - Filter by filename pattern (GDL) or sheet name (IDMC)
   - Add source filename to each row

2. **Transform:**

   - Normalize location names
   - Extract individual locations from aggregate regions
   - Match locations to GADM GIDs (or use direct ISO3/GID)
   - Verify ISO3 code consistency
   - Track unmatched locations
   - Create JSONB metadata

3. **Load:**
   - Insert into database using psycopg2
   - Use `ON CONFLICT DO UPDATE` for idempotency
   - Commit after all inserts complete

---

## Technical Implementation Details

### Why Not Multiprocessing?

Unlike `Geospatial_Lat_Long/` scripts, these ETL scripts do **not** use multiprocessing.

**Reasons:**

1. **Data volume is smaller:** Typically 1,000-50,000 rows per run (vs. millions for raster data)
2. **I/O bound:** Reading CSV/Excel files and database inserts dominate runtime
3. **Dictionary lookups are fast:** GADM name matching is O(1) after initial dictionary load
4. **Simplicity:** No need for process pool management

**Performance:** Most scripts complete in < 30 seconds on typical datasets.

---

### String Parsing Complexity

The `extract_locations()` function handles complex region descriptions:

**Example 1: Parenthetical list with multiple formats**

```python
Input: "Central (Kabul, Wardak, Kapisa, Logar Parwan Panjsher)"

Steps:
1. Extract text inside parentheses: "Kabul, Wardak, Kapisa, Logar Parwan Panjsher"
2. Detect mixed delimiters (commas and spaces)
3. Split by commas first: ["Kabul", "Wardak", "Kapisa", "Logar Parwan Panjsher"]
4. Split remaining by spaces: ["Logar", "Parwan", "Panjsher"]
5. Output: ["Kabul", "Wardak", "Kapisa", "Logar", "Parwan", "Panjsher"]
```

**Example 2: Directional prefixes**

```python
Input: "North-West (I-Arkhangelsk II-Vologda III-Murmansk)"

Steps:
1. Strip "North-West" (ignore_words filter)
2. Extract parenthetical: "I-Arkhangelsk II-Vologda III-Murmansk"
3. Remove Roman numerals: "Arkhangelsk Vologda Murmansk"
4. Split by spaces
5. Output: ["Arkhangelsk", "Vologda", "Murmansk"]
```

**Edge cases handled:**

- "County of X" → "X"
- "X region" → "X"
- "incl. Y" → "Y"
- Multiple parenthetical groups (first one used)
- Percentile indicators (filtered out)

---

### ISO3 Mapping

Several ISO3 codes require manual mapping due to:

- Political changes (Kosovo: `XKX` → `XKO`)
- Historical boundaries (pre-partition Sudan: `AB9` → `SDN`)
- Special administrative regions (Hong Kong/Macau: `HKG`/`MAC` → `CHN`)

```python
iso_map = {
    'XKX': 'XKO',  # Kosovo (non-standard → standard)
    'AB9': 'SDN',  # Sudan (pre-2011 code)
    'HKG': 'CHN',  # Hong Kong SAR
    'MAC': 'CHN'   # Macau SAR
}
```

**Applied in:** IDMC scripts only (GDL uses GADM's ISO codes directly)

---

### Metadata Preservation

All scripts store original data attributes in JSONB columns:

**GDL metadata example:**

```json
{
  "iso_code": "USA",
  "ISO2": "US",
  "iso_num": 840,
  "country": "United States",
  "year": 2019,
  "datasource": "DHS",
  "GDLCODE": "USAr101",
  "level": "Subnat",
  "region": "California"
}
```

**IDMC SADD metadata example:**

```json
{
  "ISO3": "SYR",
  "Country": "Syrian Arab Republic",
  "Year": 2023,
  "Sex": "Male",
  "Cause": "Conflict"
}
```

**WorldPop-PWD metadata example:**

```json
{
  "Lon": -118.25,
  "Lat": 34.05,
  "ISO": "USA",
  "Name": "United States",
  "PWC_Lat": 39.83,
  "PWC_Lon": -98.58,
  "Pop": 331002651,
  "Density": 35.79,
  "Area": 9147593.0
}
```

**Benefits:**

- Traceability to source data
- Flexible schema evolution
- Query metadata using JSONB operators: `metadata->>'iso_code' = 'USA'`

---

## Quality Control

### Match Rate Monitoring

After each ETL run, check for unmatched locations:

```bash
# Count unmatched locations
wc -l unmatched_locations_*.csv

# Review specific entries
cat unmatched_locations_Area.csv | grep "IND"
```

**Typical match rates:**

- GDL Area: 85-95% (varies by country naming conventions)
- GDL SHDI: 90-98% (cleaner names)
- WorldPop-PWD: 100% (direct GID usage)
- IDMC: 100% (national level only)

---

### Data Validation

**Automated checks:**

1. **ISO3 verification:** Location matches are validated against expected country
2. **Duplicate prevention:** UNIQUE constraint on (gid, admin_level, date, variable)
3. **Null handling:** Numeric variables converted with `errors='coerce'`
4. **Unit standardization:** Population values normalized to persons (not millions or thousands)

**Database constraints:**

```sql
-- Primary key ensures row uniqueness
id SERIAL PRIMARY KEY

-- Composite unique constraint prevents data duplication
UNIQUE (gid, admin_level, date, variable)

-- JSONB allows flexible metadata storage
metadata JSONB
```

---

### Common Issues

#### Issue 1: Low Match Rate

**Symptom:** Many entries in `unmatched_locations_*.csv`

**Causes:**

- Source data uses abbreviations (e.g., "CA" for California)
- Source data uses historical names not in GADM
- Source data uses non-English names without proper encoding

**Solutions:**

1. Add variant names to GADM tables:

   ```sql
   UPDATE gadm_admin1_new
   SET admin_level_1_var_name = admin_level_1_var_name || '{CA}'
   WHERE admin_level_1 = 'California' AND iso3 = 'USA';
   ```

2. Update source data to use standardized names

3. Add manual mappings to `iso_map` dictionary in script

---

#### Issue 2: Duplicate Entries

**Symptom:** `psycopg2.IntegrityError: duplicate key value violates unique constraint`

**Cause:** Source data contains duplicate rows for same (gid, admin_level, date, variable)

**Solution:** The `ON CONFLICT DO UPDATE` clause handles this automatically by updating existing rows:

```sql
INSERT INTO geospatial_data_gdl (...)
VALUES (...)
ON CONFLICT (gid, admin_level, date, variable) DO UPDATE
SET raw_value = EXCLUDED.raw_value,
    note = EXCLUDED.note,
    source = EXCLUDED.source,
    metadata = EXCLUDED.metadata
```

This allows safe re-runs of ETL scripts.

---

#### Issue 3: ISO3 Mismatch

**Symptom:** Location exists in GADM but not matching

**Example:**

```csv
ISO Code,Location,Original Region
FRA,Paris,Île-de-France
```

**Cause:** Normalization removes accents but GADM entry uses "Ile-de-France"

**Solution:**

1. Check GADM variant names:

   ```sql
   SELECT admin_level_1, admin_level_1_var_name
   FROM gadm_admin1_new
   WHERE iso3 = 'FRA' AND admin_level_1 ILIKE '%ile%';
   ```

2. If missing, add variant:
   ```sql
   UPDATE gadm_admin1_new
   SET admin_level_1_var_name = admin_level_1_var_name || '{Ile-de-France}'
   WHERE admin_level_1 = 'Île-de-France';
   ```

---

## Comparison: ISO/AdminName vs. Lat/Long

| Aspect                  | ISO/AdminName                       | Lat/Long                                  |
| ----------------------- | ----------------------------------- | ----------------------------------------- |
| **Input format**        | CSV, Excel, GeoJSON                 | NetCDF, GeoTIFF, HDF                      |
| **Location method**     | Name-based matching                 | Spatial clipping                          |
| **Processing approach** | String normalization + dictionary   | Raster statistics + area weighting        |
| **Multiprocessing**     | No (small data volumes)             | Yes (6 workers)                           |
| **Match verification**  | ISO3 code consistency check         | Geometry intersection test                |
| **Unmatched handling**  | CSV file for manual review          | Null values + missing percentage          |
| **Data volume**         | 1,000-50,000 rows per run           | Millions of grid cells                    |
| **Runtime**             | < 30 seconds                        | Minutes to hours                          |
| **Accuracy**            | Depends on name quality             | Depends on clipping method                |
| **Admin levels**        | 0 (national), 1 (state), 2 (county) | 0, 1, 2 (any GADM level)                  |
| **Resumability**        | Re-run entire script                | File tracking with processed/ folders     |
| **Primary challenge**   | Name variants and non-Latin scripts | Boundary precision and computational cost |

---

For raster-based geospatial data, see `../Geospatial_Lat_Long/README.md`.
