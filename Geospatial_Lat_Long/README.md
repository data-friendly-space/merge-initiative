# Geospatial Lat/Long Processing

This directory contains ETL scripts for processing gridded geospatial datasets (NetCDF, HDF, GeoTIFF) on latitude/longitude grids. The scripts calculate areal statistics for administrative boundaries (GADM levels 0, 1, 2) by clipping raster data to polygon geometries and computing area-weighted aggregations.

---

## Table of Contents

1. [Overview](#overview)
2. [Calculation Methodologies](#calculation-methodologies)
3. [Cell Area Calculation](#cell-area-calculation)
4. [Data Sources](#data-sources)
5. [Data Pre-processing](#data-pre-processing)
6. [Database Setup](#database-setup)
7. [Usage](#usage)
8. [Technical Implementation Details](#technical-implementation-details)
9. [Quality Control](#quality-control)
10. [Known Issues](#known-issues)
11. [Choosing Between Methods](#choosing-between-methods)

---

## Overview

### What This Does

These scripts convert **pixel-based raster data** into **polygon-aggregated statistics** suitable for administrative boundary analysis:

```
Input:  Raster grid (e.g., 0.25° × 0.25° climate data)
Output: Statistics per administrative unit (country, state, region)
        → Stored in PostgreSQL with GID, date, variable, mean, min, max
```

### Key Features

- **Area-weighted calculations** accounting for latitude-dependent cell sizes
- **Boundary handling** with two methods: `all_touched` (faster) and `area_weighting` (more accurate)
- **Multi-temporal processing** for time-series data
- **Parallel processing** using multiprocessing.Pool
- **Resumable workflows** - tracks processed files in nested folder structure
- **Missing data handling** - calculates and stores missing value percentages

---

## Calculation Methodologies

### Method 1: All-Touched (Default)

**Used by:** ERA5, GFED, GLEAM, MERRA2, WorldPop, NASA_MCD43C4, LandCover

**Approach:**

- Any raster cell that **touches** the boundary polygon is included
- Each included cell is weighted by its **full area**
- Faster, suitable for large administrative units

**Implementation:**

```python
clipped = da.rio.clip([geometry], all_touched=True)
weights = cell_area.where(clipped.notnull())
weighted_mean = (clipped * weights).sum() / weights.sum()
```

**Trade-off:**

- ✅ Computationally efficient
- ✅ Good approximation for large polygons
- ⚠️ Systematically overestimates area for small polygons
- ⚠️ Cells barely touching boundary get full weight

---

### Method 2: Area-Weighting (High Precision)

**Used by:** ERA5 (area_weighting version only)

**Approach:**

- Calculates the **exact fraction** of each boundary cell inside the polygon
- Boundary cells weighted by intersection area / total cell area
- Interior cells still use full weight
- More accurate, especially for small or irregular polygons

**Implementation:**

```python
# Step 1: Identify boundary cells
rasterized = features.rasterize([geometry], ..., all_touched=True)
boundary = find_cells_adjacent_to_exterior(rasterized)

# Step 2: Calculate exact fractions for boundary cells
for boundary_cell:
    cell_box = shapely.geometry.box(...)
    intersection = cell_box.intersection(geometry)
    fraction = intersection.area / cell_box.area

# Step 3: Weight by both area and fraction
weights = cell_area * cell_fractions
weighted_mean = (clipped * weights).sum() / weights.sum()
```

**Trade-off:**

- ✅ Geometrically precise
- ✅ No systematic bias
- ⚠️ Slower (computes intersections for boundary cells)
- ⚠️ Adds complexity

---

## Cell Area Calculation

All scripts account for **latitude-dependent cell area** using spherical approximation:

### Formula

For a cell at latitude φ with resolution Δλ × Δφ (in degrees):

```
Cell Area (m²) = R² × (π/180)² × Δλ × Δφ × cos(φ)
```

Where:

- `R = 6,371,000 m` (Earth's radius)
- `Δλ = longitude resolution` (e.g., 0.25°)
- `Δφ = latitude resolution` (e.g., 0.25°)
- `φ = latitude in degrees`

### Why This Matters

```
At equator (φ=0°):   cos(0°) = 1.00  → Full area
At 45° latitude:      cos(45°) = 0.71 → 71% of equatorial area
At 60° latitude:      cos(60°) = 0.50 → 50% of equatorial area
```

Without latitude correction, high-latitude regions would be **over-weighted** in area statistics.

### Implementation

**NetCDF (xarray):**

```python
def calculate_cell_area(da):
    res_x, res_y = da.rio.resolution()
    area_sq_degrees = np.abs(res_x * res_y)

    earth_radius = 6371000  # meters
    lat_radians = np.deg2rad(da.latitude)

    area = (area_sq_degrees * (np.pi/180)**2 *
            earth_radius**2 * np.cos(lat_radians))

    # Create 2D array: tile lat-dependent area across longitudes
    area_2d = np.tile(area, (da.sizes['longitude'], 1)).T

    return xr.DataArray(area_2d, dims=('latitude', 'longitude'), ...)
```

**GeoTIFF/HDF (rasterio):**

```python
def calculate_cell_areas(transform, shape):
    res_x = abs(transform[0])
    res_y = abs(transform[4])
    area_sq_degrees = res_x * res_y

    earth_radius = 6371000

    # Calculate latitude for each row
    lats = np.linspace(
        transform[3] + transform[4] * shape[0],  # bottom
        transform[3],                             # top
        shape[0]
    )
    lat_radians = np.deg2rad(lats)

    areas = (area_sq_degrees * (np.pi/180)**2 *
             earth_radius**2 * np.cos(lat_radians))

    # Broadcast to 2D
    area_2d = np.tile(areas[:, np.newaxis], (1, shape[1]))

    return area_2d
```

---

## Data Sources

| Source           | Format           | Variables                                   | Resolution       | Temporal        | Method                       |
| ---------------- | ---------------- | ------------------------------------------- | ---------------- | --------------- | ---------------------------- |
| **ERA5**         | NetCDF           | Precipitation, Temperature, Solar Radiation | 0.25°            | Daily           | all_touched / area_weighting |
| **GFED**         | NetCDF           | Burnt Area                                  | 0.25°            | Monthly         | all_touched                  |
| **GLEAM**        | NetCDF           | Soil Moisture, Evaporation                  | 0.25°            | Daily           | all_touched                  |
| **LandCover**    | NetCDF           | Land Cover Classes (categorical)            | 0.002778°        | Yearly          | all_touched (pixel counts)   |
| **MERRA2**       | NetCDF           | Aerosols (BC, Dust, OC)                     | 0.5° × 0.625°    | Daily           | all_touched                  |
| **WorldPop**     | NetCDF / GeoTIFF | Population Count, Density, Age/Sex          | 30 arcsec (~1km) | Yearly          | all_touched                  |
| **NASA MCD43C4** | HDF4             | NDVI (derived from reflectance)             | 0.05°            | 8-day composite | all_touched                  |

### Special Considerations

**LandCover:**

- **Categorical data** (not continuous)
- Returns pixel **counts** and **percentages** per land class
- No area weighting for mean (counts are inherently unweighted)
- Stores all classes in JSONB metadata column

**WorldPop:**

- Returns **both** unweighted sum (total population) and area-weighted mean (density)
- Sum is critical for population counts
- Age/Sex data processed separately via GeoTIFF files

**NASA MCD43C4:**

- Requires **band math** (NDVI = (NIR - Red) / (NIR + Red))
- Includes **quality filtering** (only pixels with Albedo_Quality ≤ 5)
- Uses `pyhdf` library for HDF4 format (not compatible with xarray)

---

## Data Pre-processing

Before running the main ETL scripts, raw data often needs to be prepared. Each data source includes Jupyter notebooks or Python scripts for this purpose.

### ERA5: Temporal Aggregation

**Script:** `ERA5/calculate_hourly_to_daily_ERA5_netCDF.ipynb`

**Purpose:** Convert hourly ERA5 reanalysis data to daily aggregates

**Aggregation Methods (variable-dependent):**

- **Precipitation, Evaporation, Solar Radiation** → `sum` (cumulative daily total)
- **Mean Temperature** → `mean` (daily average)
- **Maximum Temperature** → `max` (daily maximum)
- **Minimum Temperature** → `min` (daily minimum)

**Usage:**

```python
# Single file processing
netcdf_file_path = "ERA5_2020_hourly_precipitation.nc"
output = "ERA5_2020_total_precipitation_daily_aggregated.nc"

ds = xr.open_dataset(netcdf_file_path)
ds['time'] = pd.to_datetime(ds['time'].values)
ds_daily = ds.resample(time='1D').sum()  # or .mean(), .max(), .min()
ds_daily.to_netcdf(output)
```

**Batch Processing:**
The notebook includes a loop function that:

1. Auto-detects variable type from filename
2. Applies appropriate aggregation method
3. Processes all `.nc` files in a directory
4. Outputs files with naming pattern: `{original}_daily_aggregated_{method}.nc`

**Why needed:** ERA5 raw data is hourly (~8760 timesteps/year). Daily aggregation reduces file size by 24× and matches the temporal resolution needed for administrative boundary analysis.

---

### GLEAM: File Chunking

**Script:** `GLEAM/chunk_GLEAM_by_month.ipynb`

**Purpose:** Split large annual GLEAM files into monthly chunks for manageable processing

**Function:**

```python
def divide_gleam_data(input_file, output_dir):
    with xr.open_dataset(input_file) as ds:
        for month, month_ds in ds.groupby('time.month'):
            year = month_ds.time.dt.year.values[0]
            output_file = f"GLEAM_v4.1a_SMrz_data_{year}_{month:02d}.nc"
            month_ds.to_netcdf(output_file)
```

**Example:**

```
Input:  SMrz_2019_GLEAM_v4.1a.nc (4.5 GB, 365 days)
Output: GLEAM_v4.1a_SMrz_data_2019_01.nc (31 days)
        GLEAM_v4.1a_SMrz_data_2019_02.nc (28 days)
        ...
        GLEAM_v4.1a_SMrz_data_2019_12.nc (31 days)
```

**Why needed:**

- Large annual files (4-5 GB) can cause memory issues during processing
- Monthly chunks allow parallel processing of different months
- Easier to resume if processing is interrupted
- More granular file tracking in `processed/` folders

---

### MERRA2: Dual Aggregation

**Script:** `MERRA2/mean_hourly_to_daily_MERRA2_netCDF.ipynb`

**Purpose:** Aggregate hourly MERRA-2 data to daily averages AND group daily files into monthly files

**Two-step process:**

**Step 1: Hourly → Daily**

```python
# Single file: MERRA2_daily_BCSMASS.20200101.nc (24 timesteps)
ds = xr.open_dataset(file_path)
ds['time'] = pd.to_datetime(ds['time'].values)

# Apply aggregation based on variable
if var_name in ['BCSMASS', 'DUSMASS25', 'OCSMASS']:
    ds_daily = ds[var_name].resample(time='1D').mean()

ds_daily.to_netcdf(f"{file_path}_daily_aggregated_mean.nc")
```

**Step 2: Group Daily Files → Monthly**

```python
# Collect all daily files for a month
monthly_files = {
    '202001': ['MERRA2...20200101.nc', 'MERRA2...20200102.nc', ..., 'MERRA2...20200131.nc']
}

# Concatenate along time dimension
combined_ds = xr.concat([xr.open_dataset(f) for f in files], dim='time')

# Save as single monthly file
combined_ds.to_netcdf('MERRA2_BCSMASS_daily_202001.nc')
```

**Why needed:**

- MERRA-2 distributes data as one file per day with hourly timesteps
- Processing 365 individual files is inefficient
- Monthly aggregation reduces file count from 365 → 12
- Daily temporal resolution is sufficient for boundary analysis

**Output structure:**

```
Before:
  MERRA2_BCSMASS.20200101.nc (24 hours)
  MERRA2_BCSMASS.20200102.nc (24 hours)
  ... (365 files)

After:
  MERRA2_BCSMASS_daily_202001.nc (31 days)
  MERRA2_BCSMASS_daily_202002.nc (29 days)
  ... (12 files)
```

---

### Pre-processing Summary

| Source     | Script                                        | Input Frequency                       | Output Frequency | Size Reduction | Required?   |
| ---------- | --------------------------------------------- | ------------------------------------- | ---------------- | -------------- | ----------- |
| **ERA5**   | `calculate_hourly_to_daily_ERA5_netCDF.ipynb` | Hourly                                | Daily            | 24×            | Yes         |
| **GLEAM**  | `chunk_GLEAM_by_month.ipynb`                  | Annual                                | Monthly          | N/A (chunks)   | Recommended |
| **MERRA2** | `mean_hourly_to_daily_MERRA2_netCDF.ipynb`    | Hourly (per file) + Daily (365 files) | Daily (12 files) | 30×            | Yes         |
| **Others** | None                                          | N/A                                   | N/A              | N/A            | No          |

**Note:** LandCover, WorldPop, GFED, and NASA MCD43C4 do not require pre-processing - their raw data formats are already suitable for direct processing.

---

## Database Setup

Before running ETL scripts, you must create the database tables. Each data source has a corresponding `create_table_*.py` script.

### Available Scripts

```
Events/
├── EM-DAT/create_table_emdat.py
├── IDMC/create_table_gidd.py
└── merge_events/create_table_events.py

Geospatial_ISO_AdminName/
├── GDL/create_table_gdl_area.py
├── GDL/create_table_gdl_shdi.py
└── WorldPop_PWD/create_table_worldpop_pwd.py

Geospatial_Lat_Long/
├── (No create_table scripts - tables defined below)
```

### Geospatial Data Table Schema

For Lat/Long data sources, manually create tables using these templates:

**ERA5, GFED, GLEAM, MERRA2:**

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_{source} (
    id SERIAL PRIMARY KEY,
    gid TEXT NOT NULL,
    admin_level INTEGER NOT NULL,
    date DATE NOT NULL,
    variable TEXT NOT NULL,
    mean DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    missing_value_percentage DOUBLE PRECISION,
    source TEXT,
    unit TEXT,
    UNIQUE (gid, admin_level, date, variable)
);

CREATE INDEX idx_{source}_gid ON geospatial_data_{source}(gid);
CREATE INDEX idx_{source}_date ON geospatial_data_{source}(date);
CREATE INDEX idx_{source}_variable ON geospatial_data_{source}(variable);
```

**WorldPop** (includes sum column):

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_worldpop (
    id SERIAL PRIMARY KEY,
    gid TEXT NOT NULL,
    admin_level INTEGER NOT NULL,
    date DATE NOT NULL,
    variable TEXT NOT NULL,
    sum DOUBLE PRECISION,           -- Total population count
    mean DOUBLE PRECISION,          -- Area-weighted density
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    missing_value_percentage DOUBLE PRECISION,
    source TEXT,
    unit TEXT,
    UNIQUE (gid, admin_level, date, variable)
);
```

**WorldPop Age/Sex** (separate table):

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_worldpop_age_sex (
    id SERIAL PRIMARY KEY,
    gid TEXT NOT NULL,
    admin_level INTEGER NOT NULL,
    date DATE NOT NULL,
    variable TEXT NOT NULL,      -- e.g., "population_sex_age_m_0_1_count"
    sum DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    missing_value_percentage DOUBLE PRECISION,
    source TEXT,
    unit TEXT,
    UNIQUE (gid, admin_level, date, variable)
);
```

**LandCover** (includes metadata JSONB):

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_landcover (
    id SERIAL PRIMARY KEY,
    gid TEXT NOT NULL,
    admin_level INTEGER NOT NULL,
    date DATE NOT NULL,
    variable TEXT NOT NULL,      -- e.g., "10_Cropland"
    sum DOUBLE PRECISION,        -- Pixel count for this class
    mean DOUBLE PRECISION,       -- Not used
    min DOUBLE PRECISION,        -- Not used
    max DOUBLE PRECISION,        -- Not used
    raw_value DOUBLE PRECISION,  -- Percentage of total area
    missing_value_percentage DOUBLE PRECISION,
    note TEXT,
    source TEXT,
    unit TEXT,
    metadata JSONB,              -- All land classes with counts/percentages
    UNIQUE (gid, admin_level, date, variable)
);

CREATE INDEX idx_landcover_metadata ON geospatial_data_landcover USING GIN(metadata);
```

**NASA MCD43C4 NDVI:**

```sql
CREATE TABLE IF NOT EXISTS geospatial_data_nvdi (
    id SERIAL PRIMARY KEY,
    gid TEXT NOT NULL,
    admin_level INTEGER NOT NULL,
    date DATE NOT NULL,
    variable TEXT NOT NULL,
    mean DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    missing_value_percentage DOUBLE PRECISION,
    source TEXT,
    unit TEXT,
    UNIQUE (gid, admin_level, date, variable)
);
```

### Running Create Table Scripts

**Example:**

```bash
cd Events/EM-DAT/
python create_table_emdat.py

# Interactive prompts:
Enter the database password: ****
Enter the database host: localhost
```

**Best Practice:**

1. Create all necessary tables BEFORE running ETL scripts
2. Run create table scripts only once per database
3. Scripts use `CREATE TABLE IF NOT EXISTS` - safe to re-run
4. Verify tables exist: `\dt` in psql or check pgAdmin

---

## Usage

### Prerequisites

```bash
# Install dependencies
pip install xarray rioxarray geopandas numpy psycopg2 tqdm dask

# For HDF4 files (NASA_MCD43C4)
pip install pyhdf rasterio

# Set environment variable
export PROJECT_ROOT="/path/to/merge-initiative"
```

### Running a Script

**NetCDF-based (ERA5, GFED, GLEAM, MERRA2):**

```bash
cd Geospatial_Lat_Long/ERA5/
python calculate_areal_ERA5_all_touched.py

# Interactive prompts:
Enter the path to the directory containing the netCDF files: /path/to/data
Enter the path to the GeoPackage file: /path/to/gadm.gpkg
Enter the database password: ****
Enter the database host: localhost
```

**GeoTIFF-based (WorldPop Age/Sex):**

```bash
cd Geospatial_Lat_Long/WorldPop/
python calculate_areal_WorldPopAgeSex_all_touched_tif_multiprocess.py

# Prompts same as above
# Extracts year from folder name (e.g., /data/2020/ → 2020-01-01)
```

**HDF4-based (NASA MCD43C4):**

```bash
cd Geospatial_Lat_Long/NASA_MCD43C4/
python calculate_areal_NVDI.py

# Prompts same as above
```

### Processing Flow

1. **Reads administrative boundaries** from GeoPackage (`ADM_0`, `ADM_1`, `ADM_2` layers)
2. **Finds all data files** matching the variable pattern
3. **Checks processed status** - skips files already in `processed/level_X/` folders
4. **Loads raster data** into memory (with optional Dask chunking for large files)
5. **Processes geometries in batches** using multiprocessing (typically 6 workers)
6. **Inserts results into database** in chunks (100,000 rows at a time)
7. **Moves processed files** to `processed/level_X/` folder for resumability

### Output Schema

**Table:** `geospatial_data_{source}` (e.g., `geospatial_data_era5`)

| Column                     | Type    | Description                               |
| -------------------------- | ------- | ----------------------------------------- |
| `gid`                      | TEXT    | GADM geometry ID (e.g., USA.1.1_1)        |
| `admin_level`              | INTEGER | Administrative level (0, 1, 2)            |
| `date`                     | DATE    | Date of observation                       |
| `variable`                 | TEXT    | Variable name (e.g., total_precipitation) |
| `mean`                     | DOUBLE  | Area-weighted mean value                  |
| `min`                      | DOUBLE  | Minimum value within geometry             |
| `max`                      | DOUBLE  | Maximum value within geometry             |
| `sum`                      | DOUBLE  | Total sum (WorldPop only)                 |
| `missing_value_percentage` | DOUBLE  | % of cells with no data                   |
| `source`                   | TEXT    | Data source identifier                    |
| `unit`                     | TEXT    | Unit of measurement                       |
| `metadata`                 | JSONB   | Additional metadata (LandCover only)      |

**Primary Key:** `(gid, admin_level, date, variable)`

**Upsert Behavior:** `ON CONFLICT DO UPDATE` - allows re-running without duplicates

---

## Technical Implementation Details

### Buffered Pre-Clipping (Performance Optimization)

Some scripts (ERA5, LandCover, WorldPop) use **buffered bounds pre-clipping**:

```python
# Get geometry bounds
minx, miny, maxx, maxy = geometry.bounds

# Add 2-pixel buffer
buffer = max(da.rio.resolution()) * 2
minx, miny = minx - buffer, miny - buffer
maxx, maxy = maxx + buffer, maxy + buffer

# Pre-clip to buffered box (fast)
da_clipped = da.sel(
    longitude=slice(minx, maxx),
    latitude=slice(maxy, miny)
)

# Then apply precise clipping (slower, but on smaller array)
clipped = da_clipped.rio.clip([geometry], all_touched=True)
```

**Benefit:** Reduces data volume before expensive `clip()` operation
**Status:** Missing in GFED, GLEAM, MERRA2 (should be added)

---

### Coordinate Name Variations

| Script                    | Latitude             | Longitude            |
| ------------------------- | -------------------- | -------------------- |
| ERA5, WorldPop, LandCover | `latitude`           | `longitude`          |
| GFED, GLEAM, MERRA2       | `lat`                | `lon`                |
| NASA MCD43C4 (rasterio)   | N/A (uses transform) | N/A (uses transform) |

Scripts automatically handle these via `rio.set_spatial_dims()`.

---

### Multiprocessing Strategy

**Geometry-level parallelism:**

- GeoDataFrame split into batches (typically 6 batches)
- Each worker processes one batch
- Workers share read-only access to the raster DataArray

```python
with Pool(processes=6) as pool:
    batches = [gdf.iloc[i:i+batch_size] for i in range(0, len(gdf), batch_size)]

    process_batch_partial = partial(
        process_batch,
        da_daily=da_daily,  # Shared read-only
        var_name=var_name,
        level=level,
        cell_area=cell_area,
        unit=unit
    )

    for batch_result in pool.imap(process_batch_partial, batches):
        results.extend(batch_result)
```

**Why not file-level parallelism?**

- Each file contains multi-dimensional data (time × lat × lon)
- Processing order matters for database consistency
- Sequential file processing + parallel geometry processing balances load

---

### Memory Management

**Dask Chunking (Level 2+ only):**

```python
if use_dask:
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        ds_daily = xr.open_dataset(
            file_path,
            chunks={'time': 1, 'latitude': 500, 'longitude': 500}
        )
```

**Explicit Garbage Collection:**

```python
finally:
    ds_daily.close()
    gc.collect()
```

**GeoDataFrame Chunking (WorldPop Age/Sex):**

```python
for start_idx in range(0, total_features, chunk_size):
    gdf = gpd.read_file(
        geopackage_path,
        layer=f"ADM_{level}",
        rows=slice(start_idx, start_idx + chunk_size)
    )
    # Process chunk...
```

---

### File Management System

**Initial State:**

```
ERA5/
├── ERA5_2020_total_precipitation_daily_aggregated.nc
├── ERA5_2021_total_precipitation_daily_aggregated.nc
└── ERA5_2022_total_precipitation_daily_aggregated.nc
```

**After Processing Level 0:**

```
ERA5/
├── processed/
│   └── level_0/
│       ├── ERA5_2020_total_precipitation_daily_aggregated.nc
│       ├── ERA5_2021_total_precipitation_daily_aggregated.nc
│       └── ERA5_2022_total_precipitation_daily_aggregated.nc
```

**After Processing Level 1:**

```
ERA5/
├── processed/
│   └── level_1/
│       ├── ERA5_2020_total_precipitation_daily_aggregated.nc
│       ├── ERA5_2021_total_precipitation_daily_aggregated.nc
│       └── ERA5_2022_total_precipitation_daily_aggregated.nc
```

**Logic:**

- `find_files()` searches both main directory and `processed/level_X/` folders
- `get_processed_level()` extracts level from file path
- Skips files where `processed_level >= current_level`
- `move_processed_file()` relocates after successful insertion

**Benefit:** Resumable - can restart at any level without reprocessing

---

## Quality Control

### Missing Data Tracking

All scripts calculate **missing value percentage**:

```python
missing_value_percentage = (clipped.isnull().sum() / clipped.size * 100).values
```

**Interpretation:**

- `0%` = Full coverage
- `< 10%` = Excellent
- `10-30%` = Good
- `> 50%` = Sparse coverage, interpret with caution
- `100%` = No data (returns NULL for mean/min/max)

### Data Validation

**Automated checks:**

- CRS conversion to EPSG:4326
- NoData value handling (NetCDF: NaN, GeoTIFF: nodata attribute)
- Fill value masking (NASA MCD43C4: 32767)
- Quality filtering (NASA MCD43C4: Albedo_Quality ≤ 5)

**Database constraints:**

- Primary key prevents duplicate entries
- `ON CONFLICT DO UPDATE` enables idempotent re-runs
- JSONB validation for metadata columns

---

## Known Issues

### WorldPop Age/Sex Warning

**File:** `WorldPop/calculate_areal_WorldPop_all_touched.py`
**Line 1:** `# Calculation for population age sex will be wrong!`

**Issue:** This script is deprecated for age/sex data.
**Solution:** Use `calculate_areal_WorldPopAgeSex_all_touched_tif_multiprocess.py` instead.

### Performance Bottlenecks

**GFED, GLEAM, MERRA2:**

- Missing buffered pre-clipping optimization
- Can be 2-3x slower for small geometries
- **Recommendation:** Add buffered bounds clipping (see ERA5 implementation)

**LandCover:**

- Processes entire raster for each geometry
- No spatial subsetting before clipping
- Acceptable given annual frequency

---

## Choosing Between Methods

### When to Use All-Touched

- ✅ Administrative level 0 (countries) or level 1 (states/provinces)
- ✅ Large polygons (> 100 km²)
- ✅ Quick exploratory analysis
- ✅ Production pipelines where speed matters
- ✅ When cell resolution is much smaller than polygon size (e.g., 1km cells in 10,000 km² polygon)

### When to Use Area-Weighting

- ✅ Administrative level 2 (counties/districts) or smaller
- ✅ Small or irregularly shaped polygons
- ✅ Precise scientific studies
- ✅ When cell size is comparable to polygon size (e.g., 25km cells in 625 km² polygon)
- ✅ Final publication-quality results

### Error Magnitude Comparison

For a 50 km × 50 km square with 0.25° (~28 km) cells:

| Method        | Cells Included      | Area Error        |
| ------------- | ------------------- | ----------------- |
| All-touched   | ~9 (3×3 grid)       | +44% overestimate |
| Area-weighted | ~9 (with fractions) | < 1% error        |

For a 500 km × 500 km region:

| Method        | Cells Included        | Area Error       |
| ------------- | --------------------- | ---------------- |
| All-touched   | ~400 (20×20 grid)     | ~5% overestimate |
| Area-weighted | ~400 (with fractions) | < 0.1% error     |

---

## Database Performance

### Indexing Recommendations

```sql
-- Speed up spatial queries
CREATE INDEX idx_geospatial_era5_gid ON geospatial_data_era5(gid);
CREATE INDEX idx_geospatial_era5_date ON geospatial_data_era5(date);
CREATE INDEX idx_geospatial_era5_variable ON geospatial_data_era5(variable);

-- Composite index for common queries
CREATE INDEX idx_geospatial_era5_gid_date_var
ON geospatial_data_era5(gid, date, variable);

-- JSONB index for LandCover metadata queries
CREATE INDEX idx_geospatial_landcover_metadata
ON geospatial_data_landcover USING GIN(metadata);
```
