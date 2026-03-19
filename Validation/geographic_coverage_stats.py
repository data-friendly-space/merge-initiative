"""
geographic_coverage_stats.py

Generates geographic and hazard coverage statistics for the MERGE Initiative
integrated dataset.

Usage:
    python geographic_coverage_stats.py

Connects to the MERGE PostgreSQL database via config.json (using config_loader)
or falls back to interactive prompts.

Output: Prints formatted results to stdout and writes to
geographic_coverage_results.txt in the same directory.
"""

import sys
from io import StringIO
from pathlib import Path

import psycopg2

# Add project root so we can import config_loader
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def get_connection():
    """Connect to the MERGE database using config.json or interactive prompts."""
    try:
        from config_loader import CONFIG

        db_cfg = CONFIG["LOCAL_DB_CONFIG"]
        return psycopg2.connect(
            dbname=db_cfg["dbname"],
            user=db_cfg["user"],
            password=db_cfg["password"],
            host=db_cfg["host"],
        )
    except Exception:
        from getpass import getpass

        return psycopg2.connect(
            dbname="merge",
            user="postgres",
            password=getpass("Enter the database password: "),
            host=input("Enter the database host: "),
        )


def query_one(cur, sql):
    """Execute a query and return the first row."""
    cur.execute(sql)
    return cur.fetchone()


def query_all(cur, sql):
    """Execute a query and return all rows."""
    cur.execute(sql)
    return cur.fetchall()


def safe_section(name, func, cur, output):
    """Run a query section, catching errors gracefully."""
    try:
        func(cur, output)
    except Exception as e:
        output.write(f"\n[ERROR in {name}]: {e}\n")


# ---------------------------------------------------------------------------
# Section 1: Country Coverage in Events Table
# ---------------------------------------------------------------------------


def country_coverage_events(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 1: COUNTRY COVERAGE IN EVENTS TABLE\n")
    out.write("=" * 70 + "\n")

    # Total distinct countries
    row = query_one(
        cur,
        """
        SELECT COUNT(DISTINCT iso3_code) FROM events
    """,
    )
    out.write(
        f"\nTotal distinct countries (ISO3) in events table: {row[0]:,}\n"
    )

    # By source
    rows = query_all(
        cur,
        """
        SELECT source, COUNT(DISTINCT iso3_code), COUNT(*)
        FROM events
        GROUP BY source
        ORDER BY source
    """,
    )
    out.write(f"\n{'Source':<10} {'Countries':>10} {'Events':>10}\n")
    out.write("-" * 32 + "\n")
    for source, countries, events in rows:
        source_label = source if source else "NULL"
        out.write(f"{source_label:<10} {countries:>10,} {events:>10,}\n")

    # Top 20 countries by event count
    out.write("\nTop 20 countries by event count:\n")
    out.write(f"{'ISO3':<6} {'Country':<30} {'Events':>8} {'Sources':>10}\n")
    out.write("-" * 56 + "\n")
    rows = query_all(
        cur,
        """
        SELECT
            iso3_code,
            MAX(admin_level_0) AS country_name,
            COUNT(*) AS event_count,
            STRING_AGG(DISTINCT source, ', ') AS sources
        FROM events
        WHERE iso3_code IS NOT NULL
        GROUP BY iso3_code
        ORDER BY event_count DESC
        LIMIT 20
    """,
    )
    for iso3, country, count, sources in rows:
        country = (country or "")[:29]
        out.write(
            f"{iso3 or '':<6} {country:<30} {count:>8,} {sources or '':>10}\n"
        )


# ---------------------------------------------------------------------------
# Section 2: Hazard Type Coverage
# ---------------------------------------------------------------------------


def hazard_type_coverage(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 2: HAZARD TYPE COVERAGE\n")
    out.write("=" * 70 + "\n")

    # Disaster group distribution
    out.write("\n--- Disaster Group Distribution ---\n")
    rows = query_all(
        cur,
        """
        SELECT
            COALESCE(disaster_group, '[not classified]') AS grp,
            COUNT(*) AS cnt
        FROM events
        GROUP BY disaster_group
        ORDER BY cnt DESC
    """,
    )
    out.write(f"{'Disaster Group':<25} {'Events':>8}\n")
    out.write("-" * 35 + "\n")
    for grp, cnt in rows:
        out.write(f"{grp:<25} {cnt:>8,}\n")

    # Disaster type distribution
    out.write("\n--- Disaster Type Distribution ---\n")
    rows = query_all(
        cur,
        """
        SELECT
            COALESCE(disaster_type, '[null]') AS dtype,
            COUNT(*) AS cnt,
            COUNT(DISTINCT iso3_code) AS countries
        FROM events
        GROUP BY disaster_type
        ORDER BY cnt DESC
    """,
    )
    out.write(f"{'Disaster Type':<30} {'Events':>8} {'Countries':>10}\n")
    out.write("-" * 50 + "\n")
    for dtype, cnt, countries in rows:
        out.write(f"{dtype:<30} {cnt:>8,} {countries:>10,}\n")

    # Disaster subtype distribution (top 25)
    out.write("\n--- Top 25 Disaster Subtypes ---\n")
    rows = query_all(
        cur,
        """
        SELECT
            COALESCE(disaster_type, '[null]') AS dtype,
            COALESCE(disaster_subtype, '[null]') AS subtype,
            COUNT(*) AS cnt
        FROM events
        GROUP BY disaster_type, disaster_subtype
        ORDER BY cnt DESC
        LIMIT 25
    """,
    )
    out.write(f"{'Disaster Type':<25} {'Subtype':<30} {'Events':>8}\n")
    out.write("-" * 65 + "\n")
    for dtype, subtype, cnt in rows:
        out.write(f"{dtype:<25} {subtype:<30} {cnt:>8,}\n")


# ---------------------------------------------------------------------------
# Section 3: Geospatial Data Country Coverage
# ---------------------------------------------------------------------------

GEOSPATIAL_TABLES = [
    "geospatial_data_era5",
    "geospatial_data_merra2",
    "geospatial_data_gfed",
    "geospatial_data_gleam",
    "geospatial_data_nvdi",
    "geospatial_data_landcover",
    "geospatial_data_worldpop",
    "geospatial_data_worldpop_age_sex",
    "geospatial_data_gdl",
    "geospatial_data_idmc",
    "geospatial_data_worldpop_pwd",
]


def geospatial_country_coverage(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 3: GEOSPATIAL DATA COUNTRY COVERAGE\n")
    out.write("=" * 70 + "\n")

    out.write(
        f"\n{'Table':<35} {'Records':>12} {'Countries':>10} "
        f"{'Admin Levels':>13} {'GIDs':>10}\n"
    )
    out.write("-" * 82 + "\n")

    for table in GEOSPATIAL_TABLES:
        try:
            row = query_one(
                cur,
                f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT SUBSTRING(gid FROM 1 FOR 3)),
                    STRING_AGG(DISTINCT admin_level::text, ', '
                        ORDER BY admin_level::text),
                    COUNT(DISTINCT gid)
                FROM {table}
            """,
            )
            total, countries, levels, gids = row
            levels = levels or "N/A"
            out.write(
                f"{table:<35} {total:>12,} {countries:>10,} "
                f"{levels:>13} {gids:>10,}\n"
            )
        except psycopg2.errors.UndefinedTable:
            cur.connection.rollback()
            out.write(f"{table:<35} [TABLE NOT FOUND]\n")
        except Exception as e:
            cur.connection.rollback()
            out.write(f"{table:<35} [ERROR: {e}]\n")


# ---------------------------------------------------------------------------
# Section 4: Cross-Source Country Overlap
# ---------------------------------------------------------------------------


def cross_source_overlap(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 4: CROSS-SOURCE COUNTRY OVERLAP\n")
    out.write("=" * 70 + "\n")

    try:
        # Countries in events that also have geospatial data
        row = query_one(
            cur,
            """
            WITH event_countries AS (
                SELECT DISTINCT iso3_code FROM events
                WHERE iso3_code IS NOT NULL
            ),
            geo_countries AS (
                SELECT DISTINCT SUBSTRING(gid FROM 1 FOR 3) AS iso3
                FROM geospatial_data_era5
            )
            SELECT
                (SELECT COUNT(*) FROM event_countries) AS event_only,
                (SELECT COUNT(*) FROM geo_countries) AS geo_only,
                (SELECT COUNT(*) FROM event_countries ec
                 JOIN geo_countries gc ON ec.iso3_code = gc.iso3) AS both_sources
        """,
        )
        out.write(f"\nCountries with event data: {row[0]:,}\n")
        out.write(f"Countries with ERA5 geospatial data: {row[1]:,}\n")
        out.write(f"Countries with both event AND ERA5 data: {row[2]:,}\n")
    except Exception as e:
        out.write(f"\n[ERROR: {e}]\n")

    # Events per continent/region (if admin_level_0 available)
    try:
        out.write("\n--- Events by World Region (top regions) ---\n")
        rows = query_all(
            cur,
            """
            SELECT
                iso3_code,
                MAX(admin_level_0) AS country_name,
                COUNT(*) AS event_count,
                COUNT(DISTINCT disaster_type) AS hazard_types
            FROM events
            WHERE iso3_code IS NOT NULL
            GROUP BY iso3_code
            ORDER BY event_count DESC
            LIMIT 30
        """,
        )
        out.write(f"{'Country':<30} {'Events':>8} {'Hazard Types':>13}\n")
        out.write("-" * 53 + "\n")
        for iso3, country, events, hazards in rows:
            out.write(f"{(country or ''):<30} {events:>8,} {hazards:>13,}\n")
    except Exception as e:
        out.write(f"\n[ERROR: {e}]\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    conn = get_connection()
    cur = conn.cursor()
    output = StringIO()

    output.write(
        "MERGE Initiative -- Geographic and Hazard Coverage Statistics\n"
    )
    output.write(f"{'=' * 70}\n")
    output.write("Generated by geographic_coverage_stats.py\n")

    safe_section(
        "Country Coverage (Events)", country_coverage_events, cur, output
    )
    safe_section("Hazard Type Coverage", hazard_type_coverage, cur, output)
    safe_section(
        "Geospatial Country Coverage", geospatial_country_coverage, cur, output
    )
    safe_section("Cross-Source Overlap", cross_source_overlap, cur, output)

    result = output.getvalue()
    print(result)

    output_path = Path(__file__).parent / "geographic_coverage_results.txt"
    with open(output_path, "w") as f:
        f.write(result)
    print(f"\nResults saved to: {output_path}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
