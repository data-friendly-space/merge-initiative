# Geospatial Lat/Long Transformation Summary

This table summarizes all transformations applied when processing gridded raster datasets (NetCDF, GeoTIFF, HDF) into PostgreSQL tables with area-weighted statistics per GADM administrative boundary.

> For detailed methodology, calculation methods, and usage instructions, see [README.md](README.md).

---

## Pre-processing Transformations

### ERA5: Hourly to Daily

| Aggregation Type | Variables                                                           | Method                   | Example Before | Example After   |
| ---------------- | ------------------------------------------------------------------- | ------------------------ | -------------- | --------------- |
| Sum              | `total_precipitation`, `evaporation`, `surface_net_solar_radiation` | Sum 24 hourly values     | 24 values/day  | 1 daily total   |
| Mean             | `2m_temperature`                                                    | Average 24 hourly values | 24 values/day  | 1 daily average |
| Max              | `maximum_2m_temperature`                                            | Max of 24 hourly values  | 24 values/day  | 1 daily maximum |
| Min              | `minimum_2m_temperature`                                            | Min of 24 hourly values  | 24 values/day  | 1 daily minimum |

### GLEAM: Annual to Monthly

| Transformation | Example Before                               | Example After                                              |
| -------------- | -------------------------------------------- | ---------------------------------------------------------- |
| Split by month | `SMrz_2019_GLEAM_v4.1a.nc` (365 days, 4.5GB) | `GLEAM_v4.1a_SMrz_data_2019_01.nc` ... `_12.nc` (12 files) |

### MERRA-2: Hourly Files to Daily to Monthly

| Step        | Transformation                  | Example Before    | Example After       |
| ----------- | ------------------------------- | ----------------- | ------------------- |
| 1. Temporal | Hourly → Daily mean             | 24 timesteps/file | 1 timestep/file     |
| 2. Combine  | 31 daily files → 1 monthly file | 31 files          | 1 file with 31 days |

---

## Data Source Transformations

### ERA5

| Original             | Final                      | Transformation                    | Example Before                   | Example After         |
| -------------------- | -------------------------- | --------------------------------- | -------------------------------- | --------------------- |
| Variable code (`tp`) | `variable`                 | Map to full name                  | `tp`                             | `total_precipitation` |
| Time coordinate      | `date`                     | Extract date                      | `2020-03-15T12:00:00`            | `2020-03-15`          |
| GID column           | `gid`                      | From GADM layer                   | `GID_0` or `GID_1`               | `USA` or `USA.5_1`    |
| Raster values        | `mean`                     | Area-weighted mean across polygon | Grid values                      | Single float          |
| Raster values        | `min`                      | Minimum value in polygon          | Grid values                      | Single float          |
| Raster values        | `max`                      | Maximum value in polygon          | Grid values                      | Single float          |
| Null cells           | `missing_value_percentage` | % of cells with no data           | (null_count / total_count) × 100 | `53.17`               |
| NetCDF attributes    | `unit`                     | From `units` attribute            | —                                | `m` or `K`            |
| —                    | `source`                   | Constant                          | —                                | `ERA5`                |

**Variable mapping:**

| Code   | Variable Name                                           | Unit |
| ------ | ------------------------------------------------------- | ---- |
| `tp`   | `total_precipitation`                                   | m    |
| `e`    | `evaporation`                                           | m    |
| `t2m`  | `2m_temperature`                                        | K    |
| `mx2t` | `maximum_2m_temperature_since_previous_post_processing` | K    |
| `mn2t` | `minimum_2m_temperature_since_previous_post_processing` | K    |
| `ssr`  | `surface_net_solar_radiation`                           | J/m² |

### MERRA-2

| Original        | Final                | Transformation           | Example Before | Example After                     |
| --------------- | -------------------- | ------------------------ | -------------- | --------------------------------- |
| Variable code   | `variable`           | Direct use               | —              | `BCSMASS`, `DUSMASS25`, `OCSMASS` |
| Time coordinate | `date`               | Extract date             | Same as ERA5   | `2020-03-15`                      |
| Raster values   | `mean`, `min`, `max` | Area-weighted statistics | Same as ERA5   | Single floats                     |
| —               | `source`             | Constant                 | —              | `MERRA2`                          |

**Variables:**

| Code        | Description                               | Unit  |
| ----------- | ----------------------------------------- | ----- |
| `BCSMASS`   | Black Carbon Surface Mass Concentration   | kg/m³ |
| `DUSMASS25` | Dust Surface Mass Concentration PM2.5     | kg/m³ |
| `OCSMASS`   | Organic Carbon Surface Mass Concentration | kg/m³ |

### GFED

| Original                | Final                | Transformation           | Example Before | Example After                 |
| ----------------------- | -------------------- | ------------------------ | -------------- | ----------------------------- |
| Variable code (`Total`) | `variable`           | Map to full name         | `Total`        | `Monthly Burnt Area (Total)`  |
| Time coordinate         | `date`               | Extract date             | Same as ERA5   | `2020-03-01`                  |
| Raster values           | `mean`, `min`, `max` | Area-weighted statistics | Same as ERA5   | Single floats                 |
| —                       | `source`             | Constant                 | —              | `GFED_Version_0.1_2023-02-23` |

### GLEAM

| Original               | Final                | Transformation           | Example Before | Example After |
| ---------------------- | -------------------- | ------------------------ | -------------- | ------------- |
| Variable code (`SMrz`) | `variable`           | Root zone soil moisture  | —              | `SMrz`        |
| Time coordinate        | `date`               | Extract date             | Same as ERA5   | `2020-03-15`  |
| Raster values          | `mean`, `min`, `max` | Area-weighted statistics | Same as ERA5   | Single floats |
| —                      | `source`             | Constant                 | —              | `GLEAM_v4.1a` |

### WorldPop

| Original      | Final      | Transformation                    | Example Before | Example After      |
| ------------- | ---------- | --------------------------------- | -------------- | ------------------ |
| Band 1 values | `sum`      | Unweighted sum (total population) | Grid values    | `37522376`         |
| Band 1 values | `mean`     | Area-weighted mean (density)      | Grid values    | `245.6`            |
| Folder name   | `date`     | Extract year from folder name     | `/data/2020/`  | `2020-01-01`       |
| —             | `variable` | From filename parsing             | —              | See patterns below |
| —             | `source`   | Constant                          | —              | `WorldPop`         |

**Age/Sex variable naming (from GeoTIFF filenames):**

| Filename Pattern                                 | Variable Name                                          |
| ------------------------------------------------ | ------------------------------------------------------ |
| `{country}_{sex}_{min}_{max}_{year}.tif`         | `population_sex_age_{sex}_{min}_count`                 |
| `{country}_{sex}_{min}_{max}_{extra}_{year}.tif` | `2021_2022_population_sex_age_{sex}_{min}_{max}_count` |

### NASA MCD43C4 (NDVI)

| Original                    | Final      | Transformation                            | Example Before           | Example After   |
| --------------------------- | ---------- | ----------------------------------------- | ------------------------ | --------------- |
| Band 1 (Red) + Band 2 (NIR) | `mean`     | NDVI = (NIR - Red) / (NIR + Red)          | Reflectance bands        | `-1.0` to `1.0` |
| Albedo_Quality band         | —          | Filter: keep only pixels with quality ≤ 5 | Quality values           | Masked          |
| Fill value (32767)          | —          | Mask as no-data                           | `32767`                  | `NaN`           |
| Filename                    | `date`     | Parse date from filename                  | `MCD43C4.A2020001.*.hdf` | `2020-01-01`    |
| —                           | `variable` | Constant                                  | —                        | `NDVI`          |
| —                           | `source`   | Constant                                  | —                        | `NASA_MCD43C4`  |

### LandCover

| Original             | Final       | Transformation         | Example Before                    | Example After                            |
| -------------------- | ----------- | ---------------------- | --------------------------------- | ---------------------------------------- |
| Pixel values (0-220) | `sum`       | Pixel count per class  | Grid values                       | `1939230`                                |
| Pixel counts         | `raw_value` | Percentage of total    | (class_count / total_count) × 100 | `15.3`                                   |
| —                    | `variable`  | Class code + name      | —                                 | `10_Cropland`, `50_Urban`                |
| All class counts     | `metadata`  | JSONB with all classes | All classes                       | `{"190_urban": {"count": 1939230, ...}}` |
| —                    | `source`    | Constant               | —                                 | `Copernicus_CDS_LandClass`               |

**Land cover classes (examples):**

| Code | Class Name             |
| ---- | ---------------------- |
| 10   | Cropland               |
| 20   | Mosaic Cropland        |
| 50   | Urban                  |
| 60   | Bare/Sparse Vegetation |
| 70   | Snow and Ice           |
| 80   | Water                  |
| 90   | Herbaceous Wetland     |

---

## Spatial Processing

### Cell Area Calculation (Latitude-Dependent)

| Latitude     | cos(lat) | Relative Area |
| ------------ | -------- | ------------- |
| 0° (equator) | 1.00     | 100%          |
| 30°          | 0.87     | 87%           |
| 45°          | 0.71     | 71%           |
| 60°          | 0.50     | 50%           |
| 80°          | 0.17     | 17%           |

**Formula:** `Cell Area (m²) = R² × (π/180)² × Δλ × Δφ × cos(φ)`

### Clipping Methods

| Method         | Description                                      | Used By                                                       |
| -------------- | ------------------------------------------------ | ------------------------------------------------------------- |
| All-Touched    | Any cell that touches polygon gets full weight   | ERA5, GFED, GLEAM, MERRA-2, WorldPop, NASA MCD43C4, LandCover |
| Area-Weighting | Boundary cells weighted by intersection fraction | ERA5 (area_weighting version only)                            |

---

## CRS and Coordinate Handling

| Transformation                           | Before                  | After                             |
| ---------------------------------------- | ----------------------- | --------------------------------- |
| CRS standardization                      | Any CRS                 | EPSG:4326 (WGS84)                 |
| Coordinate naming (ERA5, WorldPop)       | `latitude`, `longitude` | `latitude`, `longitude`           |
| Coordinate naming (GFED, GLEAM, MERRA-2) | `lat`, `lon`            | Mapped to `latitude`, `longitude` |
| Coordinate naming (HDF/GeoTIFF)          | Transform matrix        | Calculated from affine transform  |

---

## Final Database Schema

| Column                     | Type               | Description                 | Example                                  |
| -------------------------- | ------------------ | --------------------------- | ---------------------------------------- |
| `gid`                      | `TEXT`             | GADM geometry ID            | `USA.5_1`                                |
| `admin_level`              | `INTEGER`          | 0=country, 1=state          | `1`                                      |
| `date`                     | `DATE`             | Observation date            | `2020-03-15`                             |
| `variable`                 | `TEXT`             | Variable name               | `total_precipitation`                    |
| `mean`                     | `DOUBLE PRECISION` | Area-weighted mean          | `0.0101`                                 |
| `min`                      | `DOUBLE PRECISION` | Minimum value               | `1.67e-16`                               |
| `max`                      | `DOUBLE PRECISION` | Maximum value               | `0.0784`                                 |
| `sum`                      | `DOUBLE PRECISION` | Total sum (WorldPop only)   | `37522376`                               |
| `missing_value_percentage` | `DOUBLE PRECISION` | % cells with no data        | `53.17`                                  |
| `source`                   | `TEXT`             | Data source                 | `ERA5`                                   |
| `unit`                     | `TEXT`             | Unit of measurement         | `m`                                      |
| `metadata`                 | `JSONB`            | Additional data (LandCover) | `{"190_urban": {"count": 1939230, ...}}` |
