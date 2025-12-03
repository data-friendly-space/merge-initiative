# GADM Data Transformation Summary

This table summarizes all transformations applied when importing GADM v4.1 GeoPackage data into PostgreSQL/PostGIS tables.

> For detailed methodology and usage instructions, see [README.md](README.md).

---

## Admin Level 0 (Country)

| Original Column | Final Column    | Transformation                                    | Example Before   | Example After                     |
| --------------- | --------------- | ------------------------------------------------- | ---------------- | --------------------------------- |
| `GID_0`         | `ISO3`          | Rename; used as primary key                       | `USA`            | `USA`                             |
| `NAME_0`        | `admin_level_0` | Rename only                                       | `United States`  | `United States`                   |
| `geometry`      | `geometry`      | Validate, clean, convert to WKB, ensure EPSG:4326 | `<MultiPolygon>` | `0106000020E6100000...` (WKB hex) |

---

## Admin Level 1 (State/Province)

| Original Column | Final Column             | Transformation                                    | Example Before   | Example After                     |
| --------------- | ------------------------ | ------------------------------------------------- | ---------------- | --------------------------------- |
| `GID_0`         | `ISO3`                   | Rename; foreign key to admin0                     | `USA`            | `USA`                             |
| `GID_1`         | `GID_1`                  | Primary key                                       | `USA.5_1`        | `USA.5_1`                         |
| `ISO_1`         | `ISO3_1`                 | Rename only                                       | `US-CA`          | `US-CA`                           |
| `NAME_1`        | `admin_level_1`          | Rename only                                       | `California`     | `California`                      |
| `VARNAME_1`     | `admin_level_1_var_name` | Split pipe-separated string into array            | `"CA\|Calif."`   | `['CA', 'Calif.']`                |
| `NL_NAME_1`     | `admin_level_1_nl_name`  | Split pipe-separated string into array            | `NA`             | `NULL`                            |
| `geometry`      | `geometry`               | Validate, clean, convert to WKB, ensure EPSG:4326 | `<MultiPolygon>` | `0106000020E6100000...` (WKB hex) |

---

## Admin Level 2 (County/District)

| Original Column | Final Column             | Transformation                                    | Example Before   | Example After                     |
| --------------- | ------------------------ | ------------------------------------------------- | ---------------- | --------------------------------- |
| `GID_0`         | `ISO3`                   | Rename; foreign key to admin0                     | `CHN`            | `CHN`                             |
| `GID_1`         | `GID_1`                  | Foreign key to admin1                             | `CHN.24_1`       | `CHN.24_1`                        |
| `GID_2`         | `GID_2`                  | Primary key                                       | `CHN.24.1_1`     | `CHN.24.1_1`                      |
| `NAME_2`        | `admin_level_2`          | Rename only                                       | `Shanghai`       | `Shanghai`                        |
| `VARNAME_2`     | `admin_level_2_var_name` | Split pipe-separated string into array            | `"Shànghǎi"`     | `['Shànghǎi']`                    |
| `NL_NAME_2`     | `admin_level_2_nl_name`  | Split pipe-separated string into array            | `"上海\|上海"`   | `['上海', '上海']`                |
| `geometry`      | `geometry`               | Validate, clean, convert to WKB, ensure EPSG:4326 | `<MultiPolygon>` | `0106000020E6100000...` (WKB hex) |

---

## Geometry Processing Steps

| Step | Action              | Details                                          |
| ---- | ------------------- | ------------------------------------------------ |
| 1    | Null check          | Rows with null geometries are logged and removed |
| 2    | Validity check      | Invalid geometries identified using `is_valid`   |
| 3    | Auto-fix            | Invalid geometries fixed using `.buffer(0)`      |
| 4    | Remove unfixable    | Geometries that cannot be fixed are removed      |
| 5    | CRS standardization | Reproject to EPSG:4326 if different              |
| 6    | WKB conversion      | Convert to Well-Known Binary (hex) format        |
