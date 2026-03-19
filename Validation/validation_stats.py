"""
validation_stats.py

Generates quantitative validation statistics for the MERGE Initiative
integrated dataset.

Usage:
    python validation_stats.py

Connects to the MERGE PostgreSQL database via config.json (using config_loader)
or falls back to interactive prompts.

Output: Prints formatted results to stdout and writes to validation_results.txt
in the same directory.
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


# ─────────────────────────────────────────────────────────────────────────────
# Section 1: Event Matching Statistics
# ─────────────────────────────────────────────────────────────────────────────


def event_matching_stats(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 1: EVENT MATCHING STATISTICS\n")
    out.write("=" * 70 + "\n")

    total_emdat = query_one(cur, "SELECT COUNT(*) FROM events_emdat")[0]
    total_idmc = query_one(cur, "SELECT COUNT(*) FROM events_idmc")[0]
    total_merged = query_one(cur, "SELECT COUNT(*) FROM events")[0]

    out.write(f"\n[PLACEHOLDER_TOTAL_EMDAT_EVENTS] = {total_emdat:,}\n")
    out.write(f"[PLACEHOLDER_TOTAL_IDMC_EVENTS] = {total_idmc:,}\n")
    out.write(f"[PLACEHOLDER_TOTAL_MERGED_EVENTS] = {total_merged:,}\n")

    # GLIDE availability in each source
    emdat_with_glide = query_one(
        cur,
        """
        SELECT COUNT(*) FROM events_emdat
        WHERE GLIDE IS NOT NULL AND array_length(GLIDE, 1) > 0
    """,
    )[0]
    emdat_without_glide = total_emdat - emdat_with_glide

    idmc_with_glide = query_one(
        cur,
        """
        SELECT COUNT(*) FROM events_idmc
        WHERE GLIDE IS NOT NULL AND array_length(GLIDE, 1) > 0
    """,
    )[0]
    idmc_without_glide = total_idmc - idmc_with_glide

    out.write(f"\n[PLACEHOLDER_EMDAT_WITH_GLIDE] = {emdat_with_glide:,}\n")
    out.write(f"[PLACEHOLDER_EMDAT_WITHOUT_GLIDE] = {emdat_without_glide:,}\n")
    out.write(f"[PLACEHOLDER_IDMC_WITH_GLIDE] = {idmc_with_glide:,}\n")
    out.write(f"[PLACEHOLDER_IDMC_WITHOUT_GLIDE] = {idmc_without_glide:,}\n")

    # GLIDE match count: IDMC events that matched an EM-DAT event
    glide_matched = query_one(
        cur,
        """
        SELECT COUNT(DISTINCT i.event_name)
        FROM events_idmc i
        WHERE EXISTS (
            SELECT 1 FROM events_emdat e
            WHERE EXISTS (
                SELECT 1
                FROM unnest(e.GLIDE) e_glide
                JOIN unnest(i.GLIDE) i_glide ON e_glide = i_glide
            )
        )
    """,
    )[0]
    glide_unmatched = total_idmc - glide_matched

    out.write(f"\n[PLACEHOLDER_GLIDE_MATCHED_COUNT] = {glide_matched:,}\n")
    out.write(
        f"[PLACEHOLDER_GLIDE_UNMATCHED_INSERTED] = {glide_unmatched:,}\n"
    )

    # Events by source in merged table
    source_counts = query_all(
        cur,
        """
        SELECT source, COUNT(*) FROM events GROUP BY source ORDER BY source
    """,
    )
    for source, count in source_counts:
        label = source.upper() if source else "NULL"
        out.write(f"[PLACEHOLDER_EVENTS_SOURCE_{label}] = {count:,}\n")

    # IDMC location extraction stats (from admin_level arrays)
    idmc_with_locations = query_one(
        cur,
        """
        SELECT COUNT(*) FROM events_idmc
        WHERE (admin_level_1 IS NOT NULL AND array_length(admin_level_1, 1) > 0)
           OR (admin_level_2 IS NOT NULL AND array_length(admin_level_2, 1) > 0)
    """,
    )[0]
    pct = (idmc_with_locations / total_idmc * 100) if total_idmc > 0 else 0

    out.write(
        f"\n[PLACEHOLDER_IDMC_WITH_LOCATIONS] = {idmc_with_locations:,}\n"
    )
    out.write(f"[PLACEHOLDER_IDMC_LOCATION_PERCENTAGE] = {pct:.1f}%\n")


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: Events Table Completeness
# ─────────────────────────────────────────────────────────────────────────────

EVENTS_COLUMNS = [
    ("event_name", "scalar"),
    ("disaster_type", "scalar"),
    ("disaster_subtype", "scalar"),
    ("iso3_code", "scalar"),
    ("admin_level_0", "scalar"),
    ("admin_level_1", "array"),
    ("admin_level_2", "array"),
    ("start_date", "scalar"),
    ("total_deaths", "scalar"),
    ("number_injured", "scalar"),
    ("number_affected", "scalar"),
    ("number_homeless", "scalar"),
    ("total_affected", "scalar"),
    ("total_damage_adjusted", "scalar"),
    ("disaster_internal_displacements", "scalar"),
    ("GLIDE", "array"),
]


def events_completeness(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 2: EVENTS TABLE COMPLETENESS\n")
    out.write("=" * 70 + "\n")

    for filter_label, where_clause in [
        ("ALL", ""),
        ("EMDAT", "WHERE source = 'EMDAT'"),
        ("IDMC", "WHERE source = 'IDMC'"),
    ]:
        total = query_one(cur, f"SELECT COUNT(*) FROM events {where_clause}")[
            0
        ]
        out.write(f"\n--- Source: {filter_label} (total: {total:,}) ---\n")
        out.write(f"{'Column':<40} {'Non-Null':>10} {'Completeness':>12}\n")
        out.write("-" * 64 + "\n")

        for col, col_type in EVENTS_COLUMNS:
            if col_type == "array":
                sql = f"""
                    SELECT SUM(CASE WHEN {col} IS NOT NULL
                        AND array_length({col}, 1) > 0 THEN 1 ELSE 0 END)
                    FROM events {where_clause}
                """
            else:
                sql = f"SELECT COUNT({col}) FROM events {where_clause}"
            nn = query_one(cur, sql)[0] or 0
            pct = (nn / total * 100) if total > 0 else 0
            out.write(f"{col:<40} {nn:>10,} {pct:>11.1f}%\n")


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: Geospatial Data Completeness
# ─────────────────────────────────────────────────────────────────────────────

RASTER_TABLES = [
    "geospatial_data_era5",
    "geospatial_data_merra2",
    "geospatial_data_gfed",
    "geospatial_data_gleam",
    "geospatial_data_nvdi",
    "geospatial_data_landcover",
    "geospatial_data_worldpop",
    "geospatial_data_worldpop_age_sex",
]

NAME_MATCHED_TABLES = [
    "geospatial_data_gdl",
    "geospatial_data_idmc",
    "geospatial_data_worldpop_pwd",
]


def geospatial_completeness(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 3: GEOSPATIAL DATA COMPLETENESS\n")
    out.write("=" * 70 + "\n")

    # Raster tables (with missing_value_percentage)
    out.write("\n--- Raster Tables (with missing_value_percentage) ---\n")
    header = (
        f"{'Table':<35} {'Records':>10} {'Avg Miss%':>10} "
        f"{'Med Miss%':>10} {'<10%':>8} {'<30%':>8} {'Vars':>6} {'GIDs':>8}\n"
    )
    out.write(header)
    out.write("-" * 97 + "\n")

    for table in RASTER_TABLES:
        try:
            row = query_one(
                cur,
                f"""
                SELECT
                    COUNT(*),
                    COALESCE(AVG(missing_value_percentage), 0),
                    COALESCE(
                        PERCENTILE_CONT(0.5) WITHIN GROUP
                        (ORDER BY missing_value_percentage), 0
                    ),
                    SUM(CASE WHEN missing_value_percentage < 10
                        THEN 1 ELSE 0 END),
                    SUM(CASE WHEN missing_value_percentage < 30
                        THEN 1 ELSE 0 END),
                    COUNT(DISTINCT variable),
                    COUNT(DISTINCT gid)
                FROM {table}
            """,
            )
            total, avg_m, med_m, u10, u30, vars_, gids = row
            u10 = u10 or 0
            u30 = u30 or 0
            out.write(
                f"{table:<35} {total:>10,} {avg_m:>9.1f}% {med_m:>9.1f}% "
                f"{u10:>8,} {u30:>8,} {vars_:>6} {gids:>8,}\n"
            )
        except psycopg2.errors.UndefinedTable:
            cur.connection.rollback()
            out.write(f"{table:<35} [TABLE NOT FOUND]\n")
        except Exception as e:
            cur.connection.rollback()
            out.write(f"{table:<35} [ERROR: {e}]\n")

    # Name-matched tables (no missing_value_percentage)
    out.write("\n--- Name-Matched Tables ---\n")
    header = f"{'Table':<35} {'Records':>10} {'Vars':>6} {'GIDs':>8} {'With Value':>12}\n"
    out.write(header)
    out.write("-" * 73 + "\n")

    for table in NAME_MATCHED_TABLES:
        try:
            row = query_one(
                cur,
                f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT variable),
                    COUNT(DISTINCT gid),
                    SUM(CASE WHEN raw_value IS NOT NULL THEN 1 ELSE 0 END)
                FROM {table}
            """,
            )
            total, vars_, gids, with_val = row
            with_val = with_val or 0
            out.write(
                f"{table:<35} {total:>10,} {vars_:>6} {gids:>8,} {with_val:>12,}\n"
            )
        except psycopg2.errors.UndefinedTable:
            cur.connection.rollback()
            out.write(f"{table:<35} [TABLE NOT FOUND]\n")
        except Exception as e:
            cur.connection.rollback()
            out.write(f"{table:<35} [ERROR: {e}]\n")


# ─────────────────────────────────────────────────────────────────────────────
# Section 4: Cross-Source Consistency Check
# ─────────────────────────────────────────────────────────────────────────────


def consistency_check(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 4: CROSS-SOURCE CONSISTENCY CHECK\n")
    out.write("=" * 70 + "\n")

    # Same-country pairs: match on GLIDE AND ISO3 to avoid multi-country
    # cross-product inflation (e.g., Hurricane Irma spans 16 countries,
    # creating 16×15 cross-pairs if not filtered).
    row = query_one(
        cur,
        """
        WITH same_country_pairs AS (
            SELECT
                e.disaster_type AS emdat_type,
                e.iso3_code AS iso3,
                EXTRACT(YEAR FROM e.start_date) AS emdat_year,
                i.disaster_type AS idmc_type,
                EXTRACT(YEAR FROM i.start_date) AS idmc_year
            FROM events_emdat e
            JOIN events_idmc i ON e.iso3_code = i.iso3_code
                AND EXISTS (
                    SELECT 1
                    FROM unnest(e.GLIDE) eg
                    JOIN unnest(i.GLIDE) ig ON eg = ig
                )
        )
        SELECT
            COUNT(*) AS total_pairs,
            SUM(CASE WHEN emdat_type = idmc_type THEN 1 ELSE 0 END),
            SUM(CASE WHEN emdat_year = idmc_year THEN 1 ELSE 0 END)
        FROM same_country_pairs
    """,
    )
    total, match_type, match_year = row
    total = total or 0
    match_type = match_type or 0
    match_year = match_year or 0

    out.write(f"\n[PLACEHOLDER_CONSISTENCY_TOTAL_PAIRS] = {total:,}\n")
    out.write(f"[PLACEHOLDER_MATCHING_DISASTER_TYPE] = {match_type:,}")
    if total > 0:
        out.write(f" ({match_type / total * 100:.1f}%)")
    out.write("\n")
    out.write(f"[PLACEHOLDER_MATCHING_YEAR] = {match_year:,}")
    if total > 0:
        out.write(f" ({match_year / total * 100:.1f}%)")
    out.write("\n")

    # Also report how many distinct GLIDE codes span multiple countries
    multi_country = query_one(
        cur,
        """
        WITH all_glide_pairs AS (
            SELECT DISTINCT g.glide_code,
                e.iso3_code AS emdat_iso3,
                i.iso3_code AS idmc_iso3
            FROM events_emdat e
            CROSS JOIN LATERAL unnest(e.glide) AS g(glide_code)
            JOIN events_idmc i ON g.glide_code = ANY(i.glide)
        ),
        glide_countries AS (
            SELECT glide_code,
                COUNT(DISTINCT emdat_iso3) + COUNT(DISTINCT idmc_iso3) AS total_countries
            FROM all_glide_pairs
            GROUP BY glide_code
        )
        SELECT
            COUNT(*) AS total_glides,
            SUM(CASE WHEN total_countries > 2 THEN 1 ELSE 0 END) AS multi_country_glides
        FROM glide_countries
    """,
    )
    total_g, multi_g = multi_country
    total_g = total_g or 0
    multi_g = multi_g or 0
    out.write(
        f"\nGLIDE codes spanning multiple countries: {multi_g:,} / {total_g:,}\n"
    )
    out.write(
        "(Consistency check uses same-country pairs to avoid cross-product inflation)\n"
    )

    # Show mismatched disaster types for review
    if total > 0:
        mismatches = query_all(
            cur,
            """
            WITH same_country_pairs AS (
                SELECT
                    e.disaster_type AS emdat_type,
                    i.disaster_type AS idmc_type
                FROM events_emdat e
                JOIN events_idmc i ON e.iso3_code = i.iso3_code
                    AND EXISTS (
                        SELECT 1
                        FROM unnest(e.GLIDE) eg
                        JOIN unnest(i.GLIDE) ig ON eg = ig
                    )
            )
            SELECT emdat_type, idmc_type, COUNT(*)
            FROM same_country_pairs
            WHERE emdat_type != idmc_type
            GROUP BY emdat_type, idmc_type
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """,
        )
        if mismatches:
            out.write("\nTop disaster type mismatches (EM-DAT vs IDMC):\n")
            for em_t, id_t, cnt in mismatches:
                out.write(f"  {em_t} vs {id_t}: {cnt:,}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Section 5: Temporal Coverage
# ─────────────────────────────────────────────────────────────────────────────


def temporal_coverage(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 5: TEMPORAL COVERAGE\n")
    out.write("=" * 70 + "\n")

    out.write(
        f"\n{'Table':<35} {'Min Date':>12} {'Max Date':>12} "
        f"{'Dates':>8} {'GIDs/ISO3s':>10}\n"
    )
    out.write("-" * 79 + "\n")

    # Events table uses start_date and iso3_code
    try:
        row = query_one(
            cur,
            """
            SELECT MIN(start_date), MAX(start_date),
                   COUNT(DISTINCT start_date), COUNT(DISTINCT iso3_code)
            FROM events
        """,
        )
        out.write(
            f"{'events':<35} {str(row[0]):>12} {str(row[1]):>12} "
            f"{row[2]:>8,} {row[3]:>10,}\n"
        )
    except Exception as e:
        out.write(f"{'events':<35} [ERROR: {e}]\n")

    # Geospatial tables use date and gid
    all_geo_tables = RASTER_TABLES + NAME_MATCHED_TABLES
    for table in all_geo_tables:
        try:
            row = query_one(
                cur,
                f"""
                SELECT MIN(date), MAX(date),
                       COUNT(DISTINCT date), COUNT(DISTINCT gid)
                FROM {table}
            """,
            )
            out.write(
                f"{table:<35} {str(row[0]):>12} {str(row[1]):>12} "
                f"{row[2]:>8,} {row[3]:>10,}\n"
            )
        except psycopg2.errors.UndefinedTable:
            cur.connection.rollback()
            out.write(f"{table:<35} [TABLE NOT FOUND]\n")
        except Exception as e:
            cur.connection.rollback()
            out.write(f"{table:<35} [ERROR: {e}]\n")


# ─────────────────────────────────────────────────────────────────────────────
# Section 6: Referential Integrity
# ─────────────────────────────────────────────────────────────────────────────


def referential_integrity(cur, out):
    out.write("\n" + "=" * 70 + "\n")
    out.write("SECTION 6: REFERENTIAL INTEGRITY\n")
    out.write("=" * 70 + "\n")

    # Events with valid GADM ISO3
    try:
        row = query_one(
            cur,
            """
            SELECT
                COUNT(*),
                SUM(CASE WHEN e.iso3_code IN (
                    SELECT iso3 FROM gadm_admin0
                ) THEN 1 ELSE 0 END)
            FROM events e
        """,
        )
        total, valid = row
        valid = valid or 0
        pct = (valid / total * 100) if total > 0 else 0
        out.write(
            f"\n[PLACEHOLDER_EVENTS_VALID_ISO3] = {valid:,} / {total:,} ({pct:.1f}%)\n"
        )
    except Exception as e:
        cur.connection.rollback()
        out.write(f"\n[PLACEHOLDER_EVENTS_VALID_ISO3] = [ERROR: {e}]\n")

    # Distinct variables across all geospatial tables
    try:
        parts = []
        for table in RASTER_TABLES + NAME_MATCHED_TABLES:
            parts.append(f"SELECT DISTINCT variable FROM {table}")
        union_sql = " UNION ".join(parts)
        row = query_one(cur, f"SELECT COUNT(*) FROM ({union_sql}) all_vars")
        out.write(f"[PLACEHOLDER_DISTINCT_VARIABLES_IN_DB] = {row[0]:,}\n")
    except Exception as e:
        cur.connection.rollback()
        out.write(f"[PLACEHOLDER_DISTINCT_VARIABLES_IN_DB] = [ERROR: {e}]\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main():
    conn = get_connection()
    cur = conn.cursor()
    output = StringIO()

    output.write("MERGE Initiative — Validation Statistics\n")
    output.write(f"{'=' * 70}\n")
    output.write("Generated by validation_stats.py\n")

    safe_section("Event Matching", event_matching_stats, cur, output)
    safe_section("Events Completeness", events_completeness, cur, output)
    safe_section(
        "Geospatial Completeness", geospatial_completeness, cur, output
    )
    safe_section("Consistency Check", consistency_check, cur, output)
    safe_section("Temporal Coverage", temporal_coverage, cur, output)
    safe_section("Referential Integrity", referential_integrity, cur, output)

    result = output.getvalue()
    print(result)

    output_path = Path(__file__).parent / "validation_results.txt"
    with open(output_path, "w") as f:
        f.write(result)
    print(f"\nResults saved to: {output_path}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
