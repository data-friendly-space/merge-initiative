# MERGE Initiative

## Overview

The MERGE Initiative combines geospatial, climate, disaster, conflict, and displacement data from 11+ global sources into a centralized PostgreSQL/PostGIS database for analysis and visualization.

### Key Features

- **Multi-source Integration**: Harmonizes data from disaster databases (EM-DAT, IDMC), climate reanalysis (ERA5, MERRA-2), land cover (Copernicus), population (WorldPop), and development indicators (GDL)
- **Geospatial Standardization**: All data linked to GADM (Global Administrative Areas) v4.1 administrative boundaries
- **Temporal Coverage**: Multi-decadal time series (depending on source)

---

## Data Sources

The pipeline integrates 11+ datasets using various access methods:

| Dataset            | Source                                                                                                      | Method              | Format         | Key Variables                                         | Temporal     | Spatial                      |
| ------------------ | ----------------------------------------------------------------------------------------------------------- | ------------------- | -------------- | ----------------------------------------------------- | ------------ | ---------------------------- |
| **GADM v4.1**      | UC Davis GeoData (https://geodata.ucdavis.edu/gadm/gadm4.1/)                                                | Web scraping + HTTP | GeoPackage     | Administrative boundaries (levels 0-2)                | Static       | Global                       |
| **EM-DAT**         | Emergency Events Database (https://public.emdat.be/)                                                        | GraphQL with auth   | Excel          | Disaster events, deaths, affected, damage             | 1900-present | Global (country/region)      |
| **IDMC-GIDD**      | Internal Displacement Monitoring Centre (https://www.internal-displacement.org/database/displacement-data/) | Direct HTTP         | Excel          | Internal displacement (conflict/disaster), SADD       | 2008-present | Global (country)             |
| **GDL**            | Global Data Lab (https://globaldatalab.org/mygdl/downloads/)                                                | Web scraping + auth | CSV            | Subnational HDI, wealth, education, health (60+ vars) | 1990-present | Subnational (150+ countries) |
| **WorldPop**       | WorldPop FTP Server (ftp.worldpop.org)                                                                      | FTP                 | GeoTIFF/NetCDF | Population count, density, age/sex, PWD               | 2000-2020    | Global (1km, 100m)           |
| **ERA5**           | Copernicus CDS (https://cds.climate.copernicus.eu/)                                                         | cdsapi client       | NetCDF         | Precipitation, temperature, evaporation, radiation    | 1950-present | Global (0.25°)               |
| **GFED5**          | Global Fire Emissions Database (https://www.globalfiredata.org/data.html)                                   | Zenodo API          | NetCDF         | Burned area                                           | 1997-present | Global (0.25°)               |
| **GLEAM v4.1a**    | GLEAM SFTP server (https://www.gleam.eu/)                                                                   | SFTP                | NetCDF         | Root zone soil moisture                               | 1980-present | Global (0.25°)               |
| **MERRA-2**        | NASA EarthData (https://urs.earthdata.nasa.gov/login)                                                       | HTTP                | NetCDF         | Aerosols (black carbon, dust, organic carbon)         | 1980-present | Global (0.5° × 0.625°)       |
| **NASA MCD43C4**   | NASA EarthData (https://urs.earthdata.nasa.gov/login)                                                       | HTTP                | HDF4           | NDVI (MODIS BRDF/Albedo)                              | 2000-present | Global (0.05°)               |
| **ESA Land Cover** | Copernicus CDS (https://cds.climate.copernicus.eu/)                                                         | cdsapi client       | NetCDF         | Land cover classes (22 categories)                    | 1992-present | Global (300m)                |

### Data Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA COLLECTION                          │
│  (GraphQL APIs, HTTP, FTP, SFTP, Zenodo, Copernicus CDS)        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                     PRE-PROCESSING                              │
│  • ERA5: Hourly → Daily aggregation                             │
│  • MERRA-2: Daily files → Monthly files                         │
│  • GLEAM: Annual files → Monthly chunks                         │
│  • Events: ISO3 mapping, admin name matching                    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                 ┌────────┴────────┐
                 │                 │
       ┌─────────▼─────────┐  ┌───▼──────────────────┐
       │  ISO/AdminName    │  │   Lat/Long Raster    │
       │  Processing       │  │   Processing         │
       │                   │  │                      │
       │ • Fuzzy matching  │  │ • Spatial clipping   │
       │ • Name normaliz.  │  │ • Area weighting     │
       │ • GID lookup      │  │ • Multiprocessing    │
       └─────────┬─────────┘  └───┬──────────────────┘
                 │                │
                 └────────┬───────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                   POSTGRESQL/POSTGIS DATABASE                   │
│  • GADM admin boundaries (gadm_admin0/1/2)                      │
│  • Events tables (events_emdat, events_idmc, events)            │
│  • Geospatial data tables (geospatial_data_*)                   │
│  • Views (events_flattened_admin, events_with_geometry)         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Getting Started

### Prerequisites

- **Python 3.12.4**
- **PostgreSQL 12+** with PostGIS extension
- **Conda** environment manager
- **System requirements**: 16GB+ RAM recommended for raster processing

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/merge-initiative.git
cd merge-initiative

# Create and activate conda environment
conda create --name .conda python=3.12.4
conda activate .conda

# Install dependencies
conda install --file requirements.txt
pip install -r requirements.txt

# Set PROJECT_ROOT environment variable (required for all scripts)
export PROJECT_ROOT="/path/to/merge-initiative"
# Add to ~/.bashrc or ~/.zshrc to persist
```

### Configuration

1. **Copy sample config**

   ```bash
   cp config.sample.json config.json
   ```

2. **Update `config.json`** with your settings:

   ```json
   {
     "LOCAL_DB_CONFIG": {
       "dbname": "merge",
       "user": "postgres",
       "password": "your_password",
       "host": "localhost",
       "port": "5432"
     },
     "GDL_FOLDER": "/path/to/gdl/data",
     "IDMC_FOLDER": "/path/to/idmc/data"
   }
   ```

3. **Create database**
   ```sql
   CREATE DATABASE merge;
   \c merge
   CREATE EXTENSION postgis;
   ```

---

## Database Setup Workflow

### Step 1: Import GADM Administrative Boundaries (Required First)

**CRITICAL**: All geospatial ETL workflows depend on GADM as the reference geography. Import GADM **before** running any other ETL scripts.

1. **Download GADM Data**

   ```bash
   # Download from GADM v4.1
   https://geodata.ucdavis.edu/gadm/gadm4.1
   ```

2. **Import to PostgreSQL**

   ```bash
   cd GADM/
   jupyter notebook gadm_to_postgreSQL.ipynb
   ```

   The notebook will:

   - Create tables: `gadm_admin0`, `gadm_admin1`, `gadm_admin2`
   - Import MultiPolygon geometries (EPSG:4326)
   - Create spatial indexes (GIST)
   - Establish foreign key relationships

3. **Verify Import**

   ```sql
   SELECT 'admin0' as level, COUNT(*) FROM gadm_admin0
   UNION ALL
   SELECT 'admin1' as level, COUNT(*) FROM gadm_admin1
   UNION ALL
   SELECT 'admin2' as level, COUNT(*) FROM gadm_admin2;

   ```

**Documentation**: [GADM/README.md](GADM/README.md) | [Transformation Summary](GADM/GADM_TRANSFORMATION_SUMMARY.md)

---

### Step 2: Create Database Tables

Each data source has a corresponding `create_table_*.py` script. Run these **before** executing ETL scripts.

**Events tables:**

```bash
cd Events/EM-DAT/
python create_table_emdat.py

cd ../IDMC/
python create_table_idmc.py
```

**Geospatial data tables:**

```bash
cd Geospatial_ISO_AdminName/GDL/
python create_table_gdl.py

cd ../WorldPop-PWD/
python create_table_worldpop_pwd.py

cd ../../Geospatial_ISO_AdminName/IDMC/
python create_table_idmc.py
```

**Note**: Lat/Long raster data tables must be created manually using SQL templates. See [Geospatial_Lat_Long/README.md](Geospatial_Lat_Long/README.md#database-setup) for schemas.

---

### Step 3: Run ETL Pipelines

Process data sources in any order after GADM import and table creation.

**Events data:**

```bash
# EM-DAT preprocessing
cd Events/EM-DAT/
jupyter notebook preprocess_emdat.ipynb
# Prompts: raw Excel file path, DB credentials

# IDMC preprocessing
cd ../IDMC/
jupyter notebook preprocess_idmc.ipynb
# Prompts: raw Excel file path, DB credentials

# Merge events
cd ../
jupyter notebook merge_events_with_seperate_events_table.ipynb
```

**ISO/AdminName data:**

```bash
cd Geospatial_ISO_AdminName/GDL/Area/
python GDL_Area_ETL.py
# Uses config.json for paths and credentials
# Outputs: unmatched_locations_Area.csv (if any)
```

**Lat/Long raster data:**

```bash
cd Geospatial_Lat_Long/ERA5/
python calculate_areal_ERA5_all_touched.py
# Interactive prompts:
# - Path to netCDF files
# - Path to GeoPackage
# - DB password and host
```

**Documentation**: See detailed README files in each subdirectory for complete instructions:

- [Events/README.md](Events/README.md) - Disaster and displacement events
- [Geospatial_ISO_AdminName/README.md](Geospatial_ISO_AdminName/README.md) - Name-based data
- [Geospatial_Lat_Long/README.md](Geospatial_Lat_Long/README.md) - Raster data processing

---

## Architecture

### System Components

```
┌────────────────────────────────────────────────────────────────┐
│                      config_loader.py                          │
│  - Centralized configuration management                        │
│  - Auto-detects PROJECT_ROOT using config.sample.json          │
│  - Provides CONFIG dictionary to all ETL scripts               │
└────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────────┐
│                    PostgreSQL/PostGIS                          │
│                                                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ GADM Tables (gadm_admin0/1/2)                             │ │
│  │ • ISO3, GID_1, GID_2 (primary keys)                       │ │
│  │ • admin_level_0/1/2 (names)                               │ │
│  │ • admin_level_X_var_name (alternative names - TEXT[])     │ │
│  │ • admin_level_X_nl_name (native language names - TEXT[])  │ │
│  │ • geometry (MultiPolygon, EPSG:4326)                      │ │
│  │ • GIST spatial indexes                                    │ │
│  │ • Foreign keys: admin1→admin0, admin2→admin0/admin1       │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Events Tables                                             │ │
│  │                                                           │ │
│  │ events_emdat (source-specific)                            │ │
│  │ • event_id, event_name, disaster_group/subgroup/type      │ │
│  │ • iso3_code, admin_level_0/1/2 (TEXT[] arrays)            │ │
│  │ • start_date, end_date                                    │ │
│  │ • impact metrics (deaths, affected, damage)               │ │
│  │ • GLIDE, USGS, DFO (TEXT[] arrays)                        │ │
│  │ • metadata (JSONB)                                        │ │
│  │                                                           │ │
│  │ events_idmc (source-specific)                             │ │
│  │ • Similar structure to events_emdat                       │ │
│  │ • disaster_internal_displacements                         │ │
│  │ • GLIDE, IFRC_Appeal_ID, Government_Assigned_Identifier   │ │
│  │                                                           │ │
│  │ events (merged)                                           │ │
│  │ • Combines EM-DAT and IDMC using GLIDE code matching      │ │
│  │ • source: 'EMDAT', 'IDMC', or both                        │ │
│  │                                                           │ │
│  │ Views:                                                    │ │
│  │ • events_flattened_admin: Unnests admin arrays            │ │
│  │ • events_with_geometry: Joins with GADM geometries        │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Geospatial Data Tables (geospatial_data_*)                │ │
│  │                                                           │ │
│  │ • gid (TEXT): GADM geometry identifier                    │ │
│  │ • admin_level (INTEGER): 0/1/2                            │ │
│  │ • date (DATE): observation date                           │ │
│  │ • variable (TEXT): metric name                            │ │
│  │ • mean, min, max (DOUBLE PRECISION): statistics           │ │
│  │ • sum (DOUBLE PRECISION): for population counts           │ │
│  │ • missing_value_percentage (DOUBLE PRECISION)             │ │
│  │ • raw_value (NUMERIC): for ISO/AdminName data             │ │
│  │ • metadata (JSONB): source-specific attributes            │ │
│  │ • source (TEXT): filename/source identifier               │ │
│  │                                                           │ │
│  │ Tables:                                                   │ │
│  │ • geospatial_data_era5, geospatial_data_merra2, etc.      │ │
│  │ • geospatial_data_gdl, geospatial_data_idmc               │ │
│  │ • geospatial_data_worldpop, geospatial_data_worldpop_pwd  │ │
│  │                                                           │ │
│  │ UNIQUE constraint: (gid, admin_level, date, variable)     │ │
│  │ Indexes: gid, date, variable, metadata (GIN)              │ │
│  └───────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

---

## Data Processing Methodologies

### 1. ISO/AdminName Processing (`Geospatial_ISO_AdminName/`)

**Workflow:**

```
Raw Data (CSV/Excel/GeoJSON)
    │
    ├─→ Extract location names and ISO3 codes
    │
    ├─→ Normalize names: lowercase, remove special chars, replace spaces with underscores
    │
    ├─→ Extract individual locations from aggregate regions
    │   Example: "Central (Kabul Wardak Kapisa)" → ["Kabul", "Wardak", "Kapisa"]
    │
    ├─→ Match to GADM using fuzzy matching
    │   • Query unique_admin1_names and unique_admin2_names dictionaries
    │   • Check main names, variant names, and native language names
    │   • Verify ISO3 code consistency
    │
    ├─→ Get GID for matched locations
    │
    ├─→ Record unmatched locations → unmatched_locations_*.csv
    │
    └─→ Insert into database with ON CONFLICT DO UPDATE
```

**Key Functions:**

- `normalize_name()`: Standardizes location names for matching
- `extract_locations()`: Parses aggregate region strings
- `get_unique_admin_names()`: Builds matching dictionaries from GADM
- `match_location()`: Returns (gid, admin_level) or None

**Data Sources:**

- GDL (Area, SHDI/SGDI, Geospatial): 60+ development indicators
- WorldPop-PWD: Population-weighted density (national/subnational)
- IDMC: Displacement statistics (national level)

**Documentation**: [Geospatial_ISO_AdminName/README.md](Geospatial_ISO_AdminName/README.md) | [Transformation Summary](Geospatial_ISO_AdminName/GEOSPATIAL_ISO_TRANSFORMATION_SUMMARY.md)

---

### 2. Lat/Long Raster Processing (`Geospatial_Lat_Long/`)

**Workflow:**

```
Raster Data (NetCDF/GeoTIFF/HDF)
    │
    ├─→ Pre-processing (if required)
    │   • ERA5: Hourly → Daily aggregation
    │   • MERRA-2: Daily files → Monthly files
    │   • GLEAM: Annual files → Monthly chunks
    │
    ├─→ Load GADM geometries from GeoPackage
    │
    ├─→ For each file and admin level:
    │   │
    │   ├─→ Open raster with xarray/rioxarray
    │   │
    │   ├─→ Calculate cell areas (latitude-weighted)
    │   │   area = res_x × res_y × (π/180)² × R² × cos(lat)
    │   │
    │   ├─→ Process geometries in parallel batches
    │   │   │
    │   │   ├─→ Clip raster to geometry (all_touched or area_weighting)
    │   │   │
    │   │   ├─→ Calculate area-weighted statistics
    │   │   │   • mean = Σ(value × area) / Σ(area)
    │   │   │   • min, max, missing_value_percentage
    │   │   │
    │   │   └─→ Return result tuples
    │   │
    │   ├─→ Batch insert results (100,000 rows at a time)
    │   │
    │   └─→ Move file to processed/level_X/ folder
    │
    └─→ Commit transaction
```

**Two Clipping Methods:**

| Method           | Speed  | Accuracy  | Use Case                                   |
| ---------------- | ------ | --------- | ------------------------------------------ |
| `all_touched`    | Fast   | Good      | Large admin units (countries, states)      |
| `area_weighting` | Slower | Excellent | Small units or precise scientific analysis |

**Key Functions:**

- `calculate_cell_area()`: Accounts for latitude-dependent cell sizes
- `process_batch()`: Parallel processing with multiprocessing.Pool
- `calculate_statistics()`: Weighted mean/min/max with missing data tracking
- `move_processed_file()`: File management for resumability

**Data Sources:**

- Climate: ERA5 (precipitation, temperature, evaporation, radiation)
- Aerosols: MERRA-2 (black carbon, dust, organic carbon)
- Land: GLEAM (soil moisture), GFED (burned area), ESA Land Cover
- Population: WorldPop (count, density, age/sex)
- Vegetation: NASA MCD43C4 (NDVI)

**Documentation**: [Geospatial_Lat_Long/README.md](Geospatial_Lat_Long/README.md) | [Transformation Summary](Geospatial_Lat_Long/GEOSPATIAL_LATLONG_TRANSFORMATION_SUMMARY.md)

---

### 3. Events Processing (`Events/`)

**Workflow:**

```
EM-DAT Raw Data                IDMC Raw Data
    │                              │
    ├─→ preprocess_emdat.ipynb     ├─→ preprocess_idmc.ipynb
    │   • Consolidate dates        │   • Extract event codes
    │   • Parse External IDs       │   • Process GLIDE numbers
    │   • Extract Admin Units      │   • Extract locations from names
    │   • ISO3 transformation      │   • Map hazard types
    │   • Admin name matching      │   • Admin name matching
    │   • Create event_name        │   • Create event_name
    │   • Convert monetary units   │   • Create metadata
    │   • Create metadata          │
    │                              │
    ▼                              ▼
events_emdat table            events_idmc table
    │                              │
    └──────────┬───────────────────┘
               │
               ├─→ merge_events_with_seperate_events_table.ipynb
               │   • Insert all EM-DAT events
               │   • Match IDMC events by GLIDE code
               │   • Update existing events with displacement data
               │   • Insert unmatched IDMC events as new records
               │
               ▼
         events table (unified)
               │
               ├─→ events_flattened_admin (view)
               │   • Unnests admin_level_1/2 arrays into rows
               │   • One row per event-location combination
               │
               └─→ events_with_geometry (view)
                   • Joins flattened events with GADM geometries
                   • Adds centroid_lat/long, geojson_polygon
```

**Key Transformations:**

- **ISO3 mapping**: HKG→CHN, MAC→CHN, XKX→XKO, AB9→SDN, SCG→SRB/MNE
- **GLIDE matching**: `FL-2020-000012-USA` (EM-DAT) ↔ `FL-2020-000012` (IDMC)
- **Admin name standardization**: Using GADM mapping files

**Documentation**: [Events/README.md](Events/README.md) | [Transformation Summary](Events/EVENTS_TRANSFORMATION_SUMMARY.md)

---

## Database Schema Summary

### Table Relationships

```
gadm_admin0 (ISO3) ←─────┐
    │                    │
    │ FK                 │ FK
    ▼                    │
gadm_admin1 (GID_1) ←────┤
    │                    │
    │ FK                 │ FK
    ▼                    │
gadm_admin2 (GID_2)      │
                         │
                         │ References via GID
                         │
geospatial_data_* (gid, admin_level, date, variable)
events (iso3_code, admin_level_0/1/2 arrays)
```

### Key Design Patterns

1. **Array columns for multi-location events**

   ```sql
   admin_level_1 TEXT[] = ['California', 'Nevada', 'Oregon']
   ```

2. **JSONB metadata for flexibility**

   ```sql
   metadata = {"DisNo": "2020-0001-USA", "Latitude": 34.05, ...}
   ```

3. **ON CONFLICT for idempotency**

   ```sql
   ON CONFLICT (gid, admin_level, date, variable) DO UPDATE SET ...
   ```

4. **GIN indexes for array and JSONB queries**
   ```sql
   CREATE INDEX idx_admin1 ON events USING GIN (admin_level_1);
   CREATE INDEX idx_metadata ON geospatial_data_gdl USING GIN (metadata);
   ```

---

## Quality Control

### Automated Validation

1. **Missing Data Tracking**

   - All raster processing calculates `missing_value_percentage`
   - Stored in database for filtering low-quality records
   - Typical thresholds: <10% excellent, 10-30% good, >50% caution

2. **Unmatched Location Reporting**

   - ISO/AdminName scripts output `unmatched_locations_*.csv`
   - Contains ISO3, location name, and original region string
   - Typical match rates: 85-98% depending on data source

3. **Database Constraints**
   - UNIQUE constraints prevent duplicate entries
   - Foreign keys maintain referential integrity
   - NOT NULL constraints on critical fields

### Manual Review Steps

1. **After Events Processing**

   ```sql
   -- Check for events without geometries
   SELECT COUNT(*) FROM events e
   LEFT JOIN gadm_admin0 g ON e.iso3_code = g.iso3
   WHERE g.iso3 IS NULL;
   ```

2. **After ISO/AdminName Processing**

   ```bash
   # Review unmatched locations
   wc -l Geospatial_ISO_AdminName/GDL/*/unmatched_locations_*.csv
   ```

3. **After Raster Processing**
   ```sql
   -- Check missing data percentages
   SELECT variable, AVG(missing_value_percentage) as avg_missing
   FROM geospatial_data_era5
   GROUP BY variable
   HAVING AVG(missing_value_percentage) > 30;
   ```

---

## Performance Considerations

### Raster Processing

- **Multiprocessing**: 6 workers by default (geometry-level parallelism)
- **Dask chunking**: For admin level 2 (50,000+ geometries)
- **Buffered pre-clipping**: Reduces data volume before expensive clip operations
- **Memory management**: Explicit garbage collection after each file
- **Resumability**: Files moved to `processed/` folders after successful processing

### Database Operations

- **Batch inserts**: `execute_batch()` with 100,000 row chunks
- **Index usage**: Query performance depends on proper indexing
- **Upserts**: `ON CONFLICT` clauses allow safe re-runs without duplicates

---

## Common Issues and Solutions

### Issue 1: Out of Memory Errors (Raster Processing)

**Symptom**: Script crashes during admin level 2 processing

**Solutions**:

- Enable Dask chunking in script
- Process fewer files per run
- Increase system RAM or use swap
- Use buffered pre-clipping (reduces data volume)

---

## Repository Structure

```
merge-initiative/
├── config.sample.json              # Configuration template
├── config_loader.py                # Centralized config management
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── GADM/
│   ├── README.md                   # GADM import documentation
│   └── gadm_to_postgreSQL.ipynb    # GADM import notebook
├── Events/
│   ├── README.md                   # Comprehensive events pipeline docs
│   ├── EM-DAT/
│   │   ├── create_table_emdat.py
│   │   ├── preprocess_emdat.ipynb
│   │   └── EMDAT_admin_area_mapping.xlsx
│   ├── IDMC/
│   │   ├── create_table_idmc.py
│   │   ├── preprocess_idmc.ipynb
│   │   ├── IDMC_admin_area_mapping.xlsx
│   │   └── Disaster Hazard Type Map.xlsx
│   └── merge_events_with_seperate_events_table.ipynb
├── Geospatial_ISO_AdminName/
│   ├── README.md                   # ISO/AdminName processing docs
│   ├── GDL/
│   │   ├── create_table_gdl.py
│   │   ├── Area/GDL_Area_ETL.py
│   │   ├── SHDI_SGDI/GDL_SHDI_SGDI_ETL.py
│   │   └── Geospatial/GDL_Geospatial_ETL.py
│   ├── WorldPop-PWD/
│   │   ├── create_table_worldpop_pwd.py
│   │   ├── WorldPop_PWD_national_ETL.py
│   │   └── WorldPop_PWD_sub_national_ETL.py
│   └── IDMC/
│       ├── create_table_idmc.py
│       ├── Country_Displacement/IDMC_Country_Displacement_ETL.py
│       └── IDPs_SADD_estimates/IDMC_IDPs_SADD_estimates_ETL.py
└── Geospatial_Lat_Long/
    ├── README.md                   # Raster processing docs
    ├── ERA5/
    │   ├── create_table_ERA5.py
    │   ├── calculate_hourly_to_daily_ERA5_netCDF.ipynb
    │   ├── calculate_areal_ERA5_all_touched.py
    │   └── calculate_areal_ERA5_area_weighting.py
    ├── MERRA2/
    │   ├── create_table_MERRA2.py
    │   ├── mean_hourly_to_daily_MERRA2_netCDF.ipynb
    │   └── calculate_areal_MERRA2_all_touched.py
    ├── GLEAM/
    │   ├── create_table_GLEAM.py
    │   ├── chunk_GLEAM_by_month.ipynb
    │   └── calculate_areal_GLEAM_all_touched.py
    ├── GFED/
    ├── LandCover/
    ├── WorldPop/
    └── NASA_MCD43C4/
```

---

## Contact

For questions or issues, please open a GitHub issue or contact MERGE Consortium.
