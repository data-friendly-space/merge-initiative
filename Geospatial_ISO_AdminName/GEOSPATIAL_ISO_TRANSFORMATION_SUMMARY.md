# Geospatial ISO/AdminName Transformation Summary

This table summarizes all transformations applied when processing name-based geospatial datasets (GDL, IDMC, WorldPop-PWD) into PostgreSQL tables linked to GADM administrative boundaries.

> For detailed methodology, matching algorithms, and usage instructions, see [README.md](README.md).

---

## Name Normalization

| Transformation                  | Example Before    | Example After   |
| ------------------------------- | ----------------- | --------------- |
| Convert to lowercase            | `"California"`    | `"california"`  |
| Remove special characters       | `"São Paulo"`     | `"sao paulo"`   |
| Replace spaces with underscores | `"sao paulo"`     | `"sao_paulo"`   |
| Strip accents/diacritics        | `"Île-de-France"` | `"iledefrance"` |
| Remove non-Latin characters     | `"北京市"`        | `""` (empty)    |

---

## Location Extraction from Aggregate Regions

| Pattern                     | Example Before                     | Example After                            |
| --------------------------- | ---------------------------------- | ---------------------------------------- |
| Parenthetical list (spaces) | `"Central (Kabul Wardak Kapisa)"`  | `['Kabul', 'Wardak', 'Kapisa']`          |
| Parenthetical list (commas) | `"Region (A, B, C)"`               | `['A', 'B', 'C']`                        |
| Comma-separated             | `"California, Oregon, Washington"` | `['California', 'Oregon', 'Washington']` |
| "and" connector             | `"Texas and Oklahoma"`             | `['Texas', 'Oklahoma']`                  |
| With Roman numerals         | `"I-Arkhangelsk II-Vologda"`       | `['Arkhangelsk', 'Vologda']`             |
| With "County of"            | `"County of Los Angeles"`          | `['Los Angeles']`                        |
| With "region" suffix        | `"California region"`              | `['California']`                         |

**Filtered terms (ignored):** North, South, East, West, Central, Total, Urban, Rural, Poor, Nonpoor, Lowest 25%, etc.

---

## GDL Transformations

| Original Column  | Final Column  | Transformation                           | Example Before           | Example After                              |
| ---------------- | ------------- | ---------------------------------------- | ------------------------ | ------------------------------------------ |
| `iso_code`       | `gid`         | Direct use for national level            | `USA`                    | `USA`                                      |
| `region`         | `gid`         | Fuzzy match to GADM GID                  | `"California"`           | `USA.5_1`                                  |
| `level`          | `admin_level` | Map: National→0, Subnat→1 or 2           | `"National"`             | `0`                                        |
| `year`           | `date`        | Convert to date format                   | `2019`                   | `2019-01-01`                               |
| `regpopm`        | `raw_value`   | Multiply by 1,000,000 (millions→persons) | `39.5`                   | `39500000`                                 |
| `pop` (SHDI)     | `raw_value`   | Multiply by 1,000 (thousands→persons)    | `331.0`                  | `331000`                                   |
| (all other vars) | `raw_value`   | Direct use                               | `0.85`                   | `0.85`                                     |
| Multiple cols    | `metadata`    | Pack into JSONB                          | `iso_code`, `year`, etc. | `{"iso_code": "AFG", ...}`                 |
| `source_file`    | `source`      | Direct use                               | `GDL-AreaData44.csv`     | `GDL-AreaData44.csv`                       |
| (extracted)      | `note`        | Add extraction context                   | —                        | `"Extracted from: Central (Kabul Wardak)"` |

---

## IDMC Transformations

| Original Column               | Final Column | Transformation           | Example Before                | Example After                               |
| ----------------------------- | ------------ | ------------------------ | ----------------------------- | ------------------------------------------- |
| `ISO3`                        | `gid`        | Direct use with mapping  | `XKX`                         | `XKO`                                       |
| `Year`                        | `date`       | Convert to date format   | `2023`                        | `2023-01-01`                                |
| `Conflict Stock Displacement` | `variable`   | Rename "Stock" → "Total" | `Conflict Stock Displacement` | `Conflict Total Displacement`               |
| `Disaster Stock Displacement` | `variable`   | Rename "Stock" → "Total" | `Disaster Stock Displacement` | `Disaster Total Displacement`               |
| `Cause` + `Sex` + Age cols    | `variable`   | Combine into pattern     | `Conflict`, `Male`, `0-4`     | `Conflict_Male_0-4`                         |
| Displacement values           | `raw_value`  | Direct use               | `50000`                       | `50000`                                     |
| Multiple cols                 | `metadata`   | Pack into JSONB          | `Sex`, `Cause`, etc.          | `{"Sex": "Male", "Cause": "Conflict", ...}` |

---

## WorldPop-PWD Transformations

### National Level

| Original Column   | Final Column  | Transformation           | Example Before            | Example After                             |
| ----------------- | ------------- | ------------------------ | ------------------------- | ----------------------------------------- |
| `ISO`             | `gid`         | Direct use               | `USA`                     | `USA`                                     |
| —                 | `admin_level` | Constant                 | —                         | `0`                                       |
| `year`            | `date`        | Convert to date format   | `2020`                    | `2020-01-01`                              |
| `PWD_G`           | `raw_value`   | Direct use (persons/km²) | `35.79`                   | `35.79`                                   |
| —                 | `variable`    | Constant                 | —                         | `Population_Weighted_Density_G`           |
| All GeoJSON props | `metadata`    | Pack into JSONB          | `Lat`, `Lon`, `Pop`, etc. | `{"ISO": "USA", "Pop": "281710914", ...}` |
| —                 | `note`        | Constant                 | —                         | `"GID directly from WorldPop"`            |

### Sub-national Level

| Original Column | Final Column  | Transformation                       | Example Before          | Example After |
| --------------- | ------------- | ------------------------------------ | ----------------------- | ------------- |
| `GID_1`         | `gid`         | Direct use (WorldPop uses GADM GIDs) | `USA.5_1`               | `USA.5_1`     |
| —               | `admin_level` | Constant                             | —                       | `1`           |
| (from filename) | `date`        | Extract year                         | `worldpop_2020.geojson` | `2020-01-01`  |
| `PWD_G`         | `raw_value`   | Direct use                           | `245.6`                 | `245.6`       |

**Note:** WorldPop uses GADM GIDs directly, so no fuzzy matching is required.

---

## ISO3 Mapping Rules

| Source ISO3 | Target ISO3 | Reason                 |
| ----------- | ----------- | ---------------------- |
| `XKX`       | `XKO`       | Kosovo standardization |
| `AB9`       | `SDN`       | Pre-2011 Sudan code    |
| `HKG`       | `CHN`       | Hong Kong SAR          |
| `MAC`       | `CHN`       | Macau SAR              |

---

## Final Database Schema

| Column        | Type          | Description                  | Example                                        |
| ------------- | ------------- | ---------------------------- | ---------------------------------------------- |
| `gid`         | `VARCHAR(15)` | GADM geometry ID or ISO3     | `AFG.10_1` or `AFG`                            |
| `admin_level` | `INTEGER`     | 0=country, 1=state, 2=county | `1`                                            |
| `date`        | `DATE`        | Observation date             | `2011-01-01`                                   |
| `variable`    | `VARCHAR(50)` | Variable name                | `iwi` or `Conflict_Male_0-4`                   |
| `raw_value`   | `NUMERIC`     | The data value               | `43.07`                                        |
| `note`        | `TEXT`        | Extraction context           | `"Extracted from: West (Karakalpakstan, ...)"` |
| `source`      | `TEXT`        | Source filename              | `GDL-AreaData44.csv`                           |
| `metadata`    | `JSONB`       | Original attributes          | `{"iso_code": "AFG", ...}`                     |
