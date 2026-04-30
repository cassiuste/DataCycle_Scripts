"""
SAC Export Script – Gold Layer → CSV
======================================
US21 - Export room presence data from gold PostgreSQL layer to CSV for SAP SAC import
US22 - Flat denormalized table optimized for SAC room presence dashboard

Output: C:/SAC_Export/room_presence_export.csv
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# =============================================================================
# CONFIGURATION
# =============================================================================

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "apartments",
    "user":     "postgres",
    "password": "admin",
}

EXPORT_DIR = Path("C:/SAC_Export")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path("C:/Logs/sac")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"sac_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# EXPORT
# =============================================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def export_room_presence():
    """
    US21 - Fully denormalized flat table for SAC room presence dashboard.
    All dimensions joined into one CSV — SAC works best with flat tables.
    """
    log.info("Exporting room presence data...")

    query = """
        SELECT
            d.full_date                         AS date,
            d.year,
            d.month,
            d.month_name,
            d.week_of_year,
            d.day_of_month,
            d.day_name,
            d.hour,
            d.is_weekend,
            d.time_of_day,

            b.house_name,
            b.city,
            b.first_name || ' ' || b.last_name  AS resident,

            r.room_name,
            r.room_type,

            p.motion_detected,
            p.presence_flag,
            p.light_lux,
            p.temperature_c

        FROM gold.fact_room_presence p
        JOIN gold.dim_date     d ON d.date_key     = p.date_key
                                AND d.hour         = p.hour
                                AND d.minute       = p.minute
        JOIN gold.dim_room     r ON r.room_key     = p.room_key
        JOIN gold.dim_building b ON b.building_key = p.building_key
        ORDER BY d.full_date, d.hour, b.house_name, r.room_name
    """

    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    conn.close()

    df = pd.DataFrame(rows)

    if df.empty:
        log.warning("  No data found in gold.fact_room_presence — is the gold ETL complete?")
        return

    log.info(f"  {len(df)} rows extracted.")

    # Convert booleans to 0/1 for SAC compatibility
    df["motion_detected"] = df["motion_detected"].astype(int)
    df["is_weekend"]      = df["is_weekend"].astype(int)

    # Format date as YYYY-MM-DD string for SAC
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Save — utf-8-sig for SAC/Excel compatibility
    output_path = EXPORT_DIR / "room_presence_export.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    log.info(f"  Saved : {output_path}")
    log.info(f"  Size  : {output_path.stat().st_size / 1024:.1f} KB")
    log.info(f"  Rows  : {len(df)}")
    log.info(f"  Cols  : {list(df.columns)}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("========================================")
    log.info("  SAC Export – Start")
    log.info(f"  Output: {EXPORT_DIR}")
    log.info("========================================\n")

    start = datetime.now()

    try:
        export_room_presence()
    except Exception as e:
        log.critical(f"Export failed: {e}", exc_info=True)
        return

    elapsed = datetime.now() - start
    log.info("\n========================================")
    log.info(f"  SAC Export Complete – elapsed: {elapsed}")
    log.info(f"  Upload this file to SAC:")
    log.info(f"    {EXPORT_DIR / 'room_presence_export.csv'}")
    log.info("========================================")


if __name__ == "__main__":
    main()