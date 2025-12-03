# GADM Data Import to PostgreSQL

This directory contains code to import GADM (Database of Global Administrative Areas) data into a PostgreSQL/PostGIS database.

> **See Also:** For a detailed summary of all column transformations with before/after examples, see [GADM_TRANSFORMATION_SUMMARY.md](GADM_TRANSFORMATION_SUMMARY.md).

## Database Setup

The script creates the following tables:

- `gadm_admin0` - Country level data

  - ISO3 (Primary key, 3-letter country code)
  - admin_level_0 (country name)
  - geometry (MultiPolygon in EPSG:4326)

- `gadm_admin1` - State/province level data

  - ISO3 (Foreign key to admin0)
  - GID_1 (Primary key)
  - ISO3_1 (Alternative identifier)
  - admin_level_1 (state/province name)
  - admin_level_1_var_name (Array of variant names)
  - admin_level_1_nl_name (Array of native language names)
  - geometry (MultiPolygon in EPSG:4326)

- `gadm_admin2` - County/district level data
  - ISO3 (Foreign key to admin0)
  - GID_1 (Foreign key to admin1)
  - GID_2 (Primary key)
  - admin_level_2 (county/district name)
  - admin_level_2_var_name (Array of variant names)
  - admin_level_2_nl_name (Array of native language names)
  - geometry (MultiPolygon in EPSG:4326)

Spatial indexes are created for efficient querying:

- GIST index on geometry column for each table (admin0, admin1, admin2)

Foreign key relationships enforce referential integrity:

- admin1 references admin0 via ISO3
- admin2 references both admin0 via ISO3 and admin1 via GID_1

## Geometry Processing

All geometry columns undergo the following processing steps before insertion:

1. **Null check** - Rows with null geometries are logged and removed
2. **Validity check** - Invalid geometries are identified using `is_valid`
3. **Auto-fix attempt** - Invalid geometries are fixed using `.buffer(0)`
4. **Remove unfixable** - Geometries that cannot be fixed are removed
5. **CRS standardization** - Data is reprojected to EPSG:4326 if different
6. **WKB conversion** - Geometries are converted to Well-Known Binary (hex) format for PostGIS insertion

## Variant Name Handling

The `VARNAME_*` and `NL_NAME_*` columns contain pipe-separated values that are split into PostgreSQL arrays:

```
Original: "CA|Calif.|Golden State"
Stored:   ['CA', 'Calif.', 'Golden State']
```

This enables flexible matching against location names from various data sources.
