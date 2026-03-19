"""
ETL Script – Bronze → Silver Layer
===================================
Sources:
  - C:/RawData/{username}/{year}/{month}/{day}/*.json   (IoT smart home)
  - C:/RawData/meteo2/{year}/{month}/{day}/Pred_*.csv   (Solar prediction)
  - C:/RawData/static/data_static.csv                   (Building metadata)

Target: PostgreSQL – schema "silver"
Strategy: Idempotent – files already recorded in silver.iot_events / meteo_solar_prediction are skipped.
"""

import os
import json
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# =============================================================================
# CONFIGURATION
# =============================================================================

RAW_DATA_ROOT = Path("C:/RawData")
STATIC_FILE   = RAW_DATA_ROOT / "static" / "data_static.csv"

# --- Sample mode ----------------------------------------------------------
# Set SAMPLE_MODE = True to only ingest the first N files per user/source.
SAMPLE_MODE        = True
SAMPLE_FILES_LIMIT = 5
# --------------------------------------------------------------------------

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "apartments",
    "user":     "postgres",
    "password": "admin",
}

# =============================================================================
# LOGGING
# =============================================================================

LOG_DIR = Path("C:/Logs/silver")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"etl_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# DATABASE HELPERS
# =============================================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def already_ingested(cur, source_file: str) -> bool:
    """Return True if this file path is already recorded in iot_events."""
    cur.execute(
        "SELECT 1 FROM silver.iot_events WHERE source_file = %s LIMIT 1",
        (source_file,)
    )
    return cur.fetchone() is not None


def already_ingested_meteo(cur, source_file: str) -> bool:
    """Return True if this file path is already recorded in meteo_solar_prediction."""
    cur.execute(
        "SELECT 1 FROM silver.meteo_solar_prediction WHERE source_file = %s LIMIT 1",
        (source_file,)
    )
    return cur.fetchone() is not None


def get_building_id(cur, username: str) -> int | None:
    """Resolve building id from username (case-insensitive match on house_name)."""
    cur.execute(
        "SELECT id_building FROM silver.dim_buildings WHERE LOWER(house_name) = LOWER(%s)",
        (username,)
    )
    row = cur.fetchone()
    return row[0] if row else None


# =============================================================================
# 1. STATIC DATA – dim_buildings
# =============================================================================

def load_static(conn):
    log.info("=== Loading static building data ===")
    if not STATIC_FILE.exists():
        log.warning(f"Static file not found: {STATIC_FILE}")
        return

    with open(STATIC_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with conn.cursor() as cur:
        for row in rows:
            # Skip empty or malformed rows
            if not row.get("idBuilding", "").strip():
                continue

            cur.execute(
                "SELECT 1 FROM silver.dim_buildings WHERE id_building = %s",
                (int(row["idBuilding"]),)
            )
            if cur.fetchone():
                log.info(f"  Building {row['idBuilding']} already exists, skipping.")
                continue

            cur.execute("""
                INSERT INTO silver.dim_buildings (
                    id_building, id_user, id_building_type, house_name,
                    first_name, last_name, latitude, longitude,
                    address, npa, city, building_year,
                    phone_number, nb_people, is_heating_on
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                int(row["idBuilding"]),
                int(row["idUser"]),
                int(row["idBuildingType"]),
                row["houseName"],
                row["firstName"],
                row["lastName"],
                float(row["latitude"]),
                float(row["longitude"]),
                row["address"],
                int(row["npa"]),
                row["city"],
                int(row["buildingYear"]) if row["buildingYear"] else None,
                row["phoneNumber"],
                int(row["nbPeople"]) if row["nbPeople"] else None,
                row["isHeatingOn"].strip() == "1",
            ))
            log.info(f"  Inserted building: {row['houseName']}")

    conn.commit()
    log.info("Static data loaded.\n")


# =============================================================================
# 2. IoT JSON FILES
# =============================================================================

def parse_datetime(dt_str: str) -> datetime:
    """Parse '31.05.2023 17:48' → datetime."""
    return datetime.strptime(dt_str, "%d.%m.%Y %H:%M")


def insert_iot_event(cur, id_building: int, username: str,
                     event_datetime: datetime, source_file: str) -> int:
    cur.execute("""
        INSERT INTO silver.iot_events (id_building, username, event_datetime, source_file)
        VALUES (%s, %s, %s, %s)
        RETURNING event_id
    """, (id_building, username, event_datetime, source_file))
    return cur.fetchone()[0]


def insert_plugs(cur, event_id: int, id_building: int, plugs: dict):
    rows = []
    for room, p in plugs.items():
        rows.append((
            event_id, id_building, room,
            p.get("timePlug"),
            p.get("power"),
            p.get("overpower"),
            p.get("is_valid"),
            p.get("counter1"),
            p.get("counter2"),
            p.get("counter3"),
            p.get("total"),
            p.get("temperature"),
            p.get("overtemperature"),
            p.get("switch"),
        ))
    if rows:
        execute_values(cur, """
            INSERT INTO silver.iot_plugs (
                event_id, id_building, room, time_plug,
                power_w, overpower_w, is_valid,
                counter1_a, counter2_a, counter3_a,
                total_wh, temperature_c, overtemperature, switch_on
            ) VALUES %s
        """, rows)


def insert_doors_windows(cur, event_id: int, id_building: int, doors_windows: dict):
    rows = []
    for room, sensors in doors_windows.items():
        for idx, s in enumerate(sensors):
            rows.append((
                event_id, id_building, room, idx,
                s.get("type"),
                s.get("switch") == "on",   # "on" = open
                s.get("battery"),
                s.get("defense"),
            ))
    if rows:
        execute_values(cur, """
            INSERT INTO silver.iot_doors_windows (
                event_id, id_building, room, sensor_index,
                sensor_type, is_open, battery_pct, defense_level
            ) VALUES %s
        """, rows)


def insert_motions(cur, event_id: int, id_building: int, motions: dict):
    rows = []
    for room, m in motions.items():
        rows.append((
            event_id, id_building, room,
            m.get("motion"),
            m.get("light"),
            m.get("temperature"),
        ))
    if rows:
        execute_values(cur, """
            INSERT INTO silver.iot_motions (
                event_id, id_building, room,
                motion_detected, light_lux, temperature_c
            ) VALUES %s
        """, rows)


def insert_meteo_indoor(cur, event_id: int, id_building: int, meteos: dict):
    """
    meteos structure: { "meteo": { "Livingroom": {...}, "Outdoor": {...}, ... } }
    """
    stations = meteos.get("meteo", {})
    rows = []
    for station, m in stations.items():
        rows.append((
            event_id, id_building, station,
            m.get("Temperature"),
            m.get("Humidity"),
            m.get("CO2"),
            m.get("Noise"),
            m.get("Pressure"),
            m.get("AbsolutePressure"),
            m.get("battery_percent"),
        ))
    if rows:
        execute_values(cur, """
            INSERT INTO silver.iot_meteo_indoor (
                event_id, id_building, station,
                temperature_c, humidity_pct, co2_ppm,
                noise_db, pressure_hpa, absolute_pressure_hpa,
                battery_pct
            ) VALUES %s
        """, rows)


def insert_humidities(cur, event_id: int, id_building: int, humidities: dict):
    rows = []
    for room, h in humidities.items():
        rows.append((
            event_id, id_building, room,
            h.get("temperature"),
            h.get("humidity"),
            h.get("devicePower"),
        ))
    if rows:
        execute_values(cur, """
            INSERT INTO silver.iot_humidities (
                event_id, id_building, room,
                temperature_c, humidity_pct, device_power_pct
            ) VALUES %s
        """, rows)


def insert_consumption(cur, event_id: int, id_building: int, consumptions: dict):
    house = consumptions.get("House", {})
    if not house:
        return
    cur.execute("""
        INSERT INTO silver.iot_consumption (
            event_id, id_building, time_consumption,
            power1_w, power2_w, power3_w, total_power_w,
            pf1, pf2, pf3,
            current1_a, current2_a, current3_a,
            voltage1_v, voltage2_v, voltage3_v,
            is_valid1, is_valid2, is_valid3,
            switch_on
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        event_id, id_building,
        house.get("timeConsumption"),
        house.get("power1"),
        house.get("power2"),
        house.get("power3"),
        house.get("total_power"),
        house.get("pf1"), house.get("pf2"), house.get("pf3"),
        house.get("current1"), house.get("current2"), house.get("current3"),
        house.get("voltage1"), house.get("voltage2"), house.get("voltage3"),
        house.get("is_valid1"), house.get("is_valid2"), house.get("is_valid3"),
        house.get("switch"),
    ))


def process_iot_file(conn, filepath: Path):
    source_file = str(filepath)

    with conn.cursor() as cur:
        if already_ingested(cur, source_file):
            log.debug(f"  SKIP (already ingested): {filepath.name}")
            return False

        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.error(f"  ERROR reading {filepath.name}: {e}")
            return False

        username    = data.get("user", "")
        dt_str      = data.get("datetime", "")
        id_building = get_building_id(cur, username)

        if id_building is None:
            log.warning(f"  No building found for user '{username}' – skipping {filepath.name}")
            return False

        try:
            event_datetime = parse_datetime(dt_str)
        except ValueError as e:
            log.error(f"  Cannot parse datetime '{dt_str}' in {filepath.name}: {e}")
            return False

        try:
            event_id = insert_iot_event(cur, id_building, username, event_datetime, source_file)

            if data.get("plugs"):
                insert_plugs(cur, event_id, id_building, data["plugs"])
            if data.get("doorsWindows"):
                insert_doors_windows(cur, event_id, id_building, data["doorsWindows"])
            if data.get("motions"):
                insert_motions(cur, event_id, id_building, data["motions"])
            if data.get("meteos"):
                insert_meteo_indoor(cur, event_id, id_building, data["meteos"])
            if data.get("humidities"):
                insert_humidities(cur, event_id, id_building, data["humidities"])
            if data.get("consumptions"):
                insert_consumption(cur, event_id, id_building, data["consumptions"])

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            log.error(f"  ERROR processing {filepath.name}: {e}")
            return False


def load_iot_files(conn):
    log.info("=== Loading IoT JSON files ===")
    total, inserted, skipped, errors = 0, 0, 0, 0

    # Walk all user folders except reserved names
    reserved = {"meteo2", "static"}
    for user_dir in RAW_DATA_ROOT.iterdir():
        if not user_dir.is_dir() or user_dir.name.lower() in reserved:
            continue

        json_files = sorted(user_dir.rglob("*.json"))
        if SAMPLE_MODE:
            json_files = json_files[:SAMPLE_FILES_LIMIT]
            log.info(f"  User: {user_dir.name} – {len(json_files)} files (SAMPLE MODE, limit={SAMPLE_FILES_LIMIT})")
        else:
            log.info(f"  User: {user_dir.name} – {len(json_files)} files found")

        for filepath in json_files:
            total += 1
            result = process_iot_file(conn, filepath)
            if result is True:
                inserted += 1
            elif result is False:
                # distinguish skip vs error via log; both count here
                skipped += 1

    log.info(f"IoT done. Total={total}  Inserted={inserted}  Skipped/Errors={skipped}\n")


# =============================================================================
# 3. METEO2 CSV FILES – meteo_solar_prediction
# =============================================================================

def process_meteo_file(conn, filepath: Path):
    source_file = str(filepath)

    with conn.cursor() as cur:
        if already_ingested_meteo(cur, source_file):
            log.debug(f"  SKIP (already ingested): {filepath.name}")
            return False

        rows = []
        try:
            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Parse timestamp – format: "2023-01-13 00:00:00+00:00"
                    try:
                        pred_time = datetime.fromisoformat(row["Time"])
                    except ValueError:
                        log.warning(f"  Cannot parse time '{row['Time']}' in {filepath.name}, skipping row.")
                        continue

                    # Only keep rows for the Sion site
                    if row.get("Site", "").strip() != "Sion":
                        continue

                    value = float(row["Value"]) if row["Value"] else None

                    # source_date from filename: Pred_2023-01-13.csv
                    try:
                        source_date = datetime.strptime(filepath.stem, "Pred_%Y-%m-%d").date()
                    except ValueError:
                        source_date = None

                    rows.append((
                        pred_time,
                        value,
                        row.get("Prediction"),
                        row.get("Site"),
                        row.get("Measurement"),
                        row.get("Unit"),
                        source_date,
                        source_file,
                    ))
        except (OSError, KeyError) as e:
            log.error(f"  ERROR reading {filepath.name}: {e}")
            return False

        if not rows:
            return False

        try:
            execute_values(cur, """
                INSERT INTO silver.meteo_solar_prediction (
                    prediction_time, value_wm2,
                    prediction_horizon, site,
                    measurement_type, unit,
                    source_date, source_file
                ) VALUES %s
            """, rows)
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            log.error(f"  ERROR inserting {filepath.name}: {e}")
            return False


def load_meteo_files(conn):
    log.info("=== Loading meteo2 solar prediction CSV files ===")
    meteo2_dir = RAW_DATA_ROOT / "meteo2"

    if not meteo2_dir.exists():
        log.warning(f"meteo2 folder not found: {meteo2_dir}")
        return

    csv_files = sorted(meteo2_dir.rglob("Pred_*.csv"))
    if SAMPLE_MODE:
        csv_files = csv_files[:SAMPLE_FILES_LIMIT]
        log.info(f"  {len(csv_files)} prediction files (SAMPLE MODE, limit={SAMPLE_FILES_LIMIT})")
    else:
        log.info(f"  {len(csv_files)} prediction files found")

    total, inserted, skipped = 0, 0, 0
    for filepath in csv_files:
        total += 1
        if process_meteo_file(conn, filepath):
            inserted += 1
        else:
            skipped += 1

    log.info(f"Meteo done. Total={total}  Inserted={inserted}  Skipped/Errors={skipped}\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("========================================")
    log.info("  ETL Silver Layer – Start")
    log.info(f"  RAW_DATA_ROOT = {RAW_DATA_ROOT}")
    log.info("========================================\n")

    start = datetime.now()

    try:
        conn = get_connection()
        log.info("Database connection established.\n")
    except Exception as e:
        log.critical(f"Cannot connect to database: {e}")
        return

    try:
        load_static(conn)
        load_iot_files(conn)
        load_meteo_files(conn)
    finally:
        conn.close()

    elapsed = datetime.now() - start
    log.info("========================================")
    log.info(f"  ETL Complete – elapsed: {elapsed}")
    log.info("========================================")


if __name__ == "__main__":
    main()
