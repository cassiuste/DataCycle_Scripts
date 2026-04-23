"""
ETL Script – Silver → Gold Layer
==================================
Reads cleaned data from the silver schema, applies data quality rules,
and loads the star schema in the gold schema.

Data Quality Rules Applied:
  - Missing Values      : Remove rows with NULL on critical fields
  - Duplicate Records   : Deduplicate on (event_id, room/equipment)
  - Incorrect Data Types: Cast and validate types on read
  - Invalid Values      : Filter out-of-range sensor readings
  - Sentinel Values     : Replace -99999 with NULL
  - Format Consistency  : Standardize timestamps to ISO, booleans to int flags
  - Data Standardization: motion → 0/1, door/window → boolean, timestamps → UTC
  - Referential Integrity: Only load rows whose building/room exists in dims
"""

import logging
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

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

LOG_DIR = Path("C:/Logs/gold")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"etl_gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# --- Sample mode --------------------------------------------------------------
SAMPLE_MODE  = True
SAMPLE_LIMIT = 100   # max rows fetched per silver table during testing
# Set SAMPLE_MODE = False when ready for full load
# ------------------------------------------------------------------------------

# =============================================================================
# THRESHOLDS – Invalid value filters
# =============================================================================

THRESHOLDS = {
    "temperature_c":  (-50,   60),     # °C  realistic indoor/outdoor range
    "humidity_pct":   (0,    100),     # %
    "co2_ppm":        (300,  5000),    # ppm
    "battery_pct":    (0,    100),     # %
    "noise_db":       (0,    130),     # dB
    "pressure_hpa":   (870,  1084),    # hPa world extremes
    "light_lux":      (0,    100000),  # lux
    "power_w":        (0,    50000),   # Watts
}

SENTINEL = -99999.0   # replaced with NULL

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
# DATA QUALITY HELPERS
# =============================================================================

def clean_float(value, field: str = None):
    """Cast to float, replace sentinel, apply threshold, return None if invalid."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None                              # Incorrect data type → NULL

    if v == SENTINEL:
        return None                              # Sentinel value → NULL

    if field and field in THRESHOLDS:
        lo, hi = THRESHOLDS[field]
        if not (lo <= v <= hi):
            return None                          # Out-of-range → NULL

    return v


def clean_int(value, field: str = None):
    """Cast to int, apply threshold, return None if invalid."""
    v = clean_float(value, field)
    return int(round(v)) if v is not None else None


def clean_bool(value):
    """Normalize various bool representations to Python bool."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "on", "yes")
    return None


def is_valid_row(row: dict, required_fields: list) -> bool:
    """Return False (discard row) if any required field is None/empty."""
    return all(row.get(f) is not None for f in required_fields)


# =============================================================================
# DATABASE
# =============================================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def sample_clause() -> str:
    return f"LIMIT {SAMPLE_LIMIT}" if SAMPLE_MODE else ""


# =============================================================================
# LOOKUP CACHES  (avoid per-row DB round-trips)
# =============================================================================

_room_cache  = {}
_equip_cache = {}


def get_room_key(cur, building_key: int, room_name: str):
    key = (building_key, room_name)
    if key not in _room_cache:
        cur.execute(
            "SELECT room_key FROM gold.dim_room "
            "WHERE building_key=%s AND room_name=%s",
            (building_key, room_name)
        )
        row = cur.fetchone()
        _room_cache[key] = row[0] if row else None
    return _room_cache[key]


def get_equipment_key(cur, room_key: int):
    if room_key not in _equip_cache:
        cur.execute(
            "SELECT equipment_key FROM gold.dim_equipment "
            "WHERE room_key=%s LIMIT 1",
            (room_key,)
        )
        row = cur.fetchone()
        _equip_cache[room_key] = row[0] if row else None
    return _equip_cache[room_key]


def get_tariff_key(cur, hour: int) -> int:
    cur.execute(
        "SELECT tariff_key FROM gold.dim_energy_tariff "
        "WHERE hour_from <= %s AND hour_to >= %s LIMIT 1",
        (hour, hour)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT tariff_key FROM gold.dim_energy_tariff LIMIT 1")
    return cur.fetchone()[0]


def slot_key(dt: datetime) -> tuple:
    """Return (date_key int, hour int, minute_slot int) for any datetime."""
    date_key = int(dt.strftime("%Y%m%d"))
    hour     = dt.hour
    minute   = (dt.minute // 15) * 15
    return date_key, hour, minute


# =============================================================================
# STEP 1 – DIMENSIONS
# =============================================================================

def load_dim_building(conn):
    log.info("  Loading dim_building ...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gold.dim_building (
                building_key, house_name, first_name, last_name,
                city, npa, latitude, longitude, building_year, nb_people
            )
            SELECT
                id_building, house_name, first_name, last_name,
                city, npa, latitude, longitude, building_year, nb_people
            FROM silver.dim_buildings
            ON CONFLICT (building_key) DO NOTHING
        """)
        log.info(f"    {cur.rowcount} buildings upserted.")
    conn.commit()


def load_dim_room(conn):
    """Collect all distinct (building, room_name) from every silver sensor table."""
    log.info("  Loading dim_room ...")

    ROOM_TYPE_MAP = {
        "livingroom": "Living",
        "office":     "Office",
        "kitchen":    "Living",
        "bdroom":     "Bedroom",
        "bhroom":     "Bathroom",
        "laundry":    "Laundry",
        "outdoor":    "Outdoor",
    }

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT e.id_building, p.room
            FROM silver.iot_plugs p
            JOIN silver.iot_events e ON e.event_id = p.event_id
            UNION
            SELECT DISTINCT e.id_building, m.room
            FROM silver.iot_motions m
            JOIN silver.iot_events e ON e.event_id = m.event_id
            UNION
            SELECT DISTINCT e.id_building, dw.room
            FROM silver.iot_doors_windows dw
            JOIN silver.iot_events e ON e.event_id = dw.event_id
            UNION
            SELECT DISTINCT e.id_building, mi.station
            FROM silver.iot_meteo_indoor mi
            JOIN silver.iot_events e ON e.event_id = mi.event_id
            UNION
            SELECT DISTINCT e.id_building, h.room
            FROM silver.iot_humidities h
            JOIN silver.iot_events e ON e.event_id = h.event_id
        """)
        rooms = cur.fetchall()

        inserted = 0
        for id_building, room_name in rooms:
            if not room_name:
                continue
            room_type = ROOM_TYPE_MAP.get(room_name.lower(), "Other")
            cur.execute("""
                INSERT INTO gold.dim_room (building_key, room_name, room_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (building_key, room_name) DO NOTHING
            """, (id_building, room_name, room_type))
            inserted += cur.rowcount

        log.info(f"    {inserted} rooms inserted.")
    conn.commit()


def load_dim_equipment(conn):
    log.info("  Loading dim_equipment ...")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gold.dim_equipment
                (building_key, room_key, equipment_name, equipment_type)
            SELECT DISTINCT
                r.building_key,
                r.room_key,
                r.room_name || ' Plug',
                'Smart Plug'
            FROM silver.iot_plugs p
            JOIN silver.iot_events e ON e.event_id    = p.event_id
            JOIN gold.dim_room r     ON r.building_key = e.id_building
                                    AND r.room_name    = p.room
            ON CONFLICT DO NOTHING
        """)
        log.info(f"    {cur.rowcount} equipment items inserted.")
    conn.commit()


def load_dim_date(conn):
    """Build dim_date from all distinct timestamps in silver (rounded to 15-min slots)."""
    log.info("  Loading dim_date ...")

    TIME_OF_DAY = [
        (0,  5,  "Night"),
        (6,  11, "Morning"),
        (12, 17, "Afternoon"),
        (18, 21, "Evening"),
        (22, 23, "Night"),
    ]

    def get_time_of_day(hour: int) -> str:
        for h_from, h_to, label in TIME_OF_DAY:
            if h_from <= hour <= h_to:
                return label
        return "Night"

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT
                date_trunc('hour', event_datetime)
                + INTERVAL '15 min'
                * FLOOR(EXTRACT(MINUTE FROM event_datetime) / 15) AS slot
            FROM silver.iot_events
            UNION
            SELECT DISTINCT
                date_trunc('hour', prediction_time)
                + INTERVAL '15 min'
                * FLOOR(EXTRACT(MINUTE FROM prediction_time) / 15)
            FROM silver.meteo_solar_prediction
        """)
        slots = [r[0] for r in cur.fetchall()]

    rows = []
    for slot in slots:
        date_key = int(slot.strftime("%Y%m%d"))
        hour     = slot.hour
        minute   = (slot.minute // 15) * 15
        rows.append((
            date_key, hour, minute,
            slot.date(),
            slot.year,
            (slot.month - 1) // 3 + 1,
            slot.month,
            slot.strftime("%B"),
            int(slot.strftime("%V")),
            slot.day,
            slot.isoweekday(),
            slot.strftime("%A"),
            slot.isoweekday() >= 6,
            get_time_of_day(hour),
            slot,
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO gold.dim_date (
                date_key, hour, minute, full_date,
                year, quarter, month, month_name,
                week_of_year, day_of_month, day_of_week, day_name,
                is_weekend, time_of_day, datetime_slot
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """, rows)
        log.info(f"    {cur.rowcount} date slots inserted.")
    conn.commit()


# =============================================================================
# STEP 2 – FACTS
# =============================================================================

def load_fact_energy(conn):
    log.info("  Loading fact_energy_consumption ...")
    limit = sample_clause()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                e.event_id, e.id_building, e.event_datetime,
                p.room,
                p.power_w, p.total_wh, p.temperature_c,
                p.switch_on,
                c.total_power_w, c.power1_w, c.power2_w, c.power3_w
            FROM silver.iot_plugs p
            JOIN silver.iot_events e ON e.event_id = p.event_id
            LEFT JOIN silver.iot_consumption c
                   ON c.event_id = p.event_id
                  AND c.id_building = p.id_building
            {limit}
        """)
        rows_raw = cur.fetchall()
        cols = [d[0] for d in cur.description]

    inserted = skipped = duplicates = 0
    seen = set()
    rows_to_insert = []

    with conn.cursor() as cur:
        for raw in rows_raw:
            row = dict(zip(cols, raw))

            # Duplicate check
            dedup_key = (row["event_id"], row["room"])
            if dedup_key in seen:
                duplicates += 1
                continue
            seen.add(dedup_key)

            # Missing values
            if not is_valid_row(row, ["event_id", "id_building", "event_datetime", "room"]):
                skipped += 1
                continue

            # Referential integrity
            room_key  = get_room_key(cur, row["id_building"], row["room"])
            equip_key = get_equipment_key(cur, room_key) if room_key else None
            if not room_key or not equip_key:
                skipped += 1
                continue

            # Type casting + invalid value filtering
            power_w   = clean_float(row["power_w"],        "power_w")
            temp_c    = clean_float(row["temperature_c"],  "temperature_c")
            house_tot = clean_float(row["total_power_w"],  "power_w")
            house_p1  = clean_float(row["power1_w"],       "power_w")
            house_p2  = clean_float(row["power2_w"],       "power_w")
            house_p3  = clean_float(row["power3_w"],       "power_w")
            power_kwh = power_w / 1000.0 if power_w is not None else None

            dt = row["event_datetime"]
            date_key, hour, minute = slot_key(dt)
            tariff_key = get_tariff_key(cur, hour)

            rows_to_insert.append((
                date_key, hour, minute,
                row["id_building"], room_key, equip_key, tariff_key,
                row["event_id"],
                power_w, power_kwh,
                row["total_wh"],
                temp_c,
                clean_bool(row["switch_on"]),
                house_tot, house_p1, house_p2, house_p3,
            ))
            inserted += 1

    if rows_to_insert:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO gold.fact_energy_consumption (
                    date_key, hour, minute,
                    building_key, room_key, equipment_key, tariff_key,
                    event_id,
                    power_w, power_kwh, total_energy_wh,
                    plug_temperature_c, switch_on,
                    house_total_power_w, house_power1_w, house_power2_w, house_power3_w
                ) VALUES %s
            """, rows_to_insert)
        conn.commit()

    log.info(f"    fact_energy_consumption: inserted={inserted}  skipped={skipped}  duplicates={duplicates}")


def load_fact_environment(conn):
    log.info("  Loading fact_environment ...")
    limit = sample_clause()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                e.event_id, e.id_building, e.event_datetime,
                mi.station,
                mi.temperature_c, mi.humidity_pct, mi.co2_ppm,
                mi.noise_db, mi.pressure_hpa, mi.absolute_pressure_hpa
            FROM silver.iot_meteo_indoor mi
            JOIN silver.iot_events e ON e.event_id = mi.event_id
            WHERE LOWER(mi.station) != 'outdoor'
            {limit}
        """)
        indoor_rows = cur.fetchall()
        indoor_cols = [d[0] for d in cur.description]

        # Outdoor values per event for denormalization
        cur.execute("""
            SELECT e.event_id, mi.temperature_c, mi.humidity_pct
            FROM silver.iot_meteo_indoor mi
            JOIN silver.iot_events e ON e.event_id = mi.event_id
            WHERE LOWER(mi.station) = 'outdoor'
        """)
        outdoor_map = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

        # Nearest solar irradiance per date+hour (Sion only)
        cur.execute("""
            SELECT
                CAST(TO_CHAR(prediction_time, 'YYYYMMDD') AS INT),
                EXTRACT(HOUR FROM prediction_time)::INT,
                AVG(CASE WHEN value_wm2 = -99999 THEN NULL ELSE value_wm2 END)
            FROM silver.meteo_solar_prediction
            WHERE site = 'Sion'
            GROUP BY 1, 2
        """)
        solar_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    inserted = skipped = duplicates = 0
    seen = set()
    rows_to_insert = []

    with conn.cursor() as cur:
        for raw in indoor_rows:
            row = dict(zip(indoor_cols, raw))

            dedup_key = (row["event_id"], row["station"])
            if dedup_key in seen:
                duplicates += 1
                continue
            seen.add(dedup_key)

            if not is_valid_row(row, ["event_id", "id_building", "event_datetime", "station"]):
                skipped += 1
                continue

            room_key = get_room_key(cur, row["id_building"], row["station"])
            if room_key is None:
                skipped += 1
                continue

            temp_c  = clean_float(row["temperature_c"], "temperature_c")
            hum     = clean_float(row["humidity_pct"],  "humidity_pct")
            co2     = clean_int(row["co2_ppm"],         "co2_ppm")
            noise   = clean_int(row["noise_db"],        "noise_db")
            press   = clean_float(row["pressure_hpa"],  "pressure_hpa")
            abs_pr  = clean_float(row["absolute_pressure_hpa"], "pressure_hpa")

            outdoor = outdoor_map.get(row["event_id"], (None, None))
            out_temp = clean_float(outdoor[0], "temperature_c")
            out_hum  = clean_float(outdoor[1], "humidity_pct")

            dt = row["event_datetime"]
            date_key, hour, minute = slot_key(dt)
            irr = clean_float(solar_map.get((date_key, hour)))

            rows_to_insert.append((
                date_key, hour, minute,
                row["id_building"], room_key,
                row["event_id"],
                temp_c, hum, co2, noise, press, abs_pr,
                out_temp, out_hum, irr,
            ))
            inserted += 1

    if rows_to_insert:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO gold.fact_environment (
                    date_key, hour, minute,
                    building_key, room_key, event_id,
                    temperature_c, humidity_pct, co2_ppm, noise_db,
                    pressure_hpa, absolute_pressure_hpa,
                    outdoor_temperature_c, outdoor_humidity_pct,
                    solar_irradiance_wm2
                ) VALUES %s
            """, rows_to_insert)
        conn.commit()

    log.info(f"    fact_environment: inserted={inserted}  skipped={skipped}  duplicates={duplicates}")


def load_fact_doors_windows(conn):
    log.info("  Loading fact_doors_windows ...")
    limit = sample_clause()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                e.event_id, e.id_building, e.event_datetime,
                dw.room, dw.sensor_type, dw.sensor_index,
                dw.is_open, dw.battery_pct
            FROM silver.iot_doors_windows dw
            JOIN silver.iot_events e ON e.event_id = dw.event_id
            {limit}
        """)
        rows_raw = cur.fetchall()
        cols = [d[0] for d in cur.description]

    inserted = skipped = duplicates = 0
    seen = set()
    rows_to_insert = []

    with conn.cursor() as cur:
        for raw in rows_raw:
            row = dict(zip(cols, raw))

            dedup_key = (row["event_id"], row["room"], row["sensor_index"])
            if dedup_key in seen:
                duplicates += 1
                continue
            seen.add(dedup_key)

            if not is_valid_row(row, ["event_id", "id_building", "event_datetime", "room"]):
                skipped += 1
                continue

            room_key = get_room_key(cur, row["id_building"], row["room"])
            if room_key is None:
                skipped += 1
                continue

            dt = row["event_datetime"]
            date_key, hour, minute = slot_key(dt)

            rows_to_insert.append((
                date_key, hour, minute,
                row["id_building"], room_key, row["event_id"],
                row["sensor_type"], row["sensor_index"],
                clean_bool(row["is_open"]),
                clean_int(row["battery_pct"], "battery_pct"),
            ))
            inserted += 1

    if rows_to_insert:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO gold.fact_doors_windows (
                    date_key, hour, minute,
                    building_key, room_key, event_id,
                    sensor_type, sensor_index,
                    is_open, battery_pct
                ) VALUES %s
            """, rows_to_insert)
        conn.commit()

    log.info(f"    fact_doors_windows: inserted={inserted}  skipped={skipped}  duplicates={duplicates}")


def load_fact_room_presence(conn):
    log.info("  Loading fact_room_presence ...")
    limit = sample_clause()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                e.event_id, e.id_building, e.event_datetime,
                m.room, m.motion_detected, m.light_lux, m.temperature_c
            FROM silver.iot_motions m
            JOIN silver.iot_events e ON e.event_id = m.event_id
            {limit}
        """)
        rows_raw = cur.fetchall()
        cols = [d[0] for d in cur.description]

    inserted = skipped = duplicates = 0
    seen = set()
    rows_to_insert = []

    with conn.cursor() as cur:
        for raw in rows_raw:
            row = dict(zip(cols, raw))

            dedup_key = (row["event_id"], row["room"])
            if dedup_key in seen:
                duplicates += 1
                continue
            seen.add(dedup_key)

            if not is_valid_row(row, ["event_id", "id_building", "event_datetime", "room"]):
                skipped += 1
                continue

            room_key = get_room_key(cur, row["id_building"], row["room"])
            if room_key is None:
                skipped += 1
                continue

            dt = row["event_datetime"]
            date_key, hour, minute = slot_key(dt)

            rows_to_insert.append((
                date_key, hour, minute,
                row["id_building"], room_key, row["event_id"],
                clean_bool(row["motion_detected"]),        # standardized boolean
                clean_int(row["light_lux"],  "light_lux"),
                clean_float(row["temperature_c"], "temperature_c"),
            ))
            inserted += 1

    if rows_to_insert:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO gold.fact_room_presence (
                    date_key, hour, minute,
                    building_key, room_key, event_id,
                    motion_detected, light_lux, temperature_c
                ) VALUES %s
            """, rows_to_insert)
        conn.commit()

    log.info(f"    fact_room_presence: inserted={inserted}  skipped={skipped}  duplicates={duplicates}")


def load_fact_solar(conn):
    log.info("  Loading fact_solar_prediction ...")
    limit = sample_clause()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT prediction_time, value_wm2, is_missing,
                   prediction_horizon, site
            FROM silver.meteo_solar_prediction
            WHERE site = 'Sion'
            {limit}
        """)
        rows_raw = cur.fetchall()
        cols = [d[0] for d in cur.description]

    inserted = skipped = duplicates = 0
    seen = set()
    rows_to_insert = []

    for raw in rows_raw:
        row = dict(zip(cols, raw))

        if not is_valid_row(row, ["prediction_time"]):
            skipped += 1
            continue

        dt = row["prediction_time"]
        date_key, hour, minute = slot_key(dt)

        dedup_key = (date_key, hour, minute)
        if dedup_key in seen:
            duplicates += 1
            continue
        seen.add(dedup_key)

        # Sentinel → NULL
        raw_val = row["value_wm2"]
        value   = None if (raw_val is None or float(raw_val) == SENTINEL) else float(raw_val)

        rows_to_insert.append((
            date_key, hour, minute,
            value,
            row["is_missing"],
            row["prediction_horizon"],
            row["site"],
        ))
        inserted += 1

    if rows_to_insert:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO gold.fact_solar_prediction (
                    date_key, hour, minute,
                    value_wm2, is_missing,
                    prediction_horizon, site
                ) VALUES %s
            """, rows_to_insert)
        conn.commit()

    log.info(f"    fact_solar_prediction: inserted={inserted}  skipped={skipped}  duplicates={duplicates}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("========================================")
    log.info("  ETL Gold Layer – Start")
    if SAMPLE_MODE:
        log.info(f"  SAMPLE MODE ON – limit={SAMPLE_LIMIT} rows per table")
    log.info("========================================\n")

    start = datetime.now()

    try:
        conn = get_connection()
        log.info("Database connection established.\n")
    except Exception as e:
        log.critical(f"Cannot connect to database: {e}")
        return

    try:
        log.info("--- STEP 1: Dimensions ---")
        load_dim_building(conn)
        load_dim_room(conn)
        load_dim_equipment(conn)
        load_dim_date(conn)

        log.info("\n--- STEP 2: Facts ---")
        load_fact_energy(conn)
        load_fact_environment(conn)
        load_fact_doors_windows(conn)
        load_fact_room_presence(conn)
        load_fact_solar(conn)

    except Exception as e:
        log.critical(f"Unexpected error: {e}", exc_info=True)
    finally:
        conn.close()

    elapsed = datetime.now() - start
    log.info("\n========================================")
    log.info(f"  ETL Gold Complete – elapsed: {elapsed}")
    log.info("========================================")


if __name__ == "__main__":
    main()
