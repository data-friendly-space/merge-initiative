# Validation

Scripts for generating quantitative validation statistics for the MERGE integrated dataset. These support the Technical Validation section of the manuscript and provide reproducible evidence for data quality claims.

## Scripts

### validation_stats.py

Data quality and matching confidence report:

- **Event matching statistics** — GLIDE-based deduplication counts between EM-DAT and IDMC
- **Record completeness** — Non-null rates for key variables in the events table, split by source
- **Geospatial data coverage** — Missing value percentages across raster and name-matched tables
- **Cross-source consistency** — Agreement rates (disaster type, ISO3, year) for GLIDE-matched event pairs
- **Temporal coverage** — Date ranges and distinct geographic units per table
- **Referential integrity** — GADM ISO3 validation and variable counts

```bash
python Validation/validation_stats.py
```

Results saved to `Validation/validation_results.txt`.

### geographic_coverage_stats.py

Country and hazard coverage inventory:

- **Country coverage** — Total countries in the events table, top 20 by event count, breakdown by source
- **Hazard type distribution** — Disaster group, type, and subtype counts
- **Geospatial country coverage** — Record counts, distinct countries, admin levels, and GIDs per table
- **Cross-source overlap** — Countries with both event and geospatial data

```bash
python Validation/geographic_coverage_stats.py
```

Results saved to `Validation/geographic_coverage_results.txt`.

## Requirements

- Python 3.8+
- `psycopg2` (already used throughout the project)
- Access to the MERGE PostgreSQL database with all tables populated

Both scripts connect via `config.json` (using `config_loader.py`). If `config.json` is not present, they fall back to interactive prompts for database host and password.
