"""
ETL Script - Silver -> Gold Layer
===================================
Reads from schema "silver", writes aggregated/normalized data into schema "gold".
Granularity: hourly + daily.
Strategy: Idempotent via INSERT ... ON CONFLICT DO UPDATE (upsert).
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

# Anomaly thresholds
CO2_HIGH_THRESHOLD      = 1000   # ppm
HUMIDITY_HIGH_THRESHOLD = 70.0   # %
BATTERY_LOW_THRESHOLD   = 20     # %
PHASE_IMBALANCE_RATIO   = 2.0    # max/min phase power ratio to flag imbalance

# =============================================================================
# LOGGING
# =============================================================================

LOG_DIR = Path("C:/Logs/gold")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"etl_gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


# =============================================================================
# 1. PLUGS AGGREGATES
# =============================================================================

def agg_plugs(conn):
    log.info("  Aggregating plugs...")
    with conn.cursor() as cur:

        # --- HOURLY ---
        cur.execute("""
            INSERT INTO gold.agg_plugs_hourly (
                id_building, room, hour_ts,
                avg_power_w, min_power_w, max_power_w,
                avg_temperature_c, pct_switch_on, sample_count
            )
            SELECT
                p.id_building,
                p.room,
                DATE_TRUNC('hour', e.event_datetime)    AS hour_ts,
                AVG(p.power_w),
                MIN(p.power_w),
                MAX(p.power_w),
                AVG(p.temperature_c),
                AVG(CASE WHEN p.switch_on THEN 1.0 ELSE 0.0 END),
                COUNT(*)
            FROM silver.iot_plugs p
            JOIN silver.iot_events e ON e.event_id = p.event_id
            GROUP BY p.id_building, p.room, DATE_TRUNC('hour', e.event_datetime)
            ON CONFLICT (id_building, room, hour_ts) DO UPDATE SET
                avg_power_w       = EXCLUDED.avg_power_w,
                min_power_w       = EXCLUDED.min_power_w,
                max_power_w       = EXCLUDED.max_power_w,
                avg_temperature_c = EXCLUDED.avg_temperature_c,
                pct_switch_on     = EXCLUDED.pct_switch_on,
                sample_count      = EXCLUDED.sample_count,
                ingested_at       = CURRENT_TIMESTAMP
        """)

        # --- DAILY ---
        cur.execute("""
            INSERT INTO gold.agg_plugs_daily (
                id_building, room, day_date,
                avg_power_w, min_power_w, max_power_w,
                avg_temperature_c, pct_switch_on, sample_count
            )
            SELECT
                p.id_building,
                p.room,
                DATE_TRUNC('day', e.event_datetime)::DATE   AS day_date,
                AVG(p.power_w),
                MIN(p.power_w),
                MAX(p.power_w),
                AVG(p.temperature_c),
                AVG(CASE WHEN p.switch_on THEN 1.0 ELSE 0.0 END),
                COUNT(*)
            FROM silver.iot_plugs p
            JOIN silver.iot_events e ON e.event_id = p.event_id
            GROUP BY p.id_building, p.room, DATE_TRUNC('day', e.event_datetime)::DATE
            ON CONFLICT (id_building, room, day_date) DO UPDATE SET
                avg_power_w       = EXCLUDED.avg_power_w,
                min_power_w       = EXCLUDED.min_power_w,
                max_power_w       = EXCLUDED.max_power_w,
                avg_temperature_c = EXCLUDED.avg_temperature_c,
                pct_switch_on     = EXCLUDED.pct_switch_on,
                sample_count      = EXCLUDED.sample_count,
                ingested_at       = CURRENT_TIMESTAMP
        """)

    conn.commit()
    log.info("  Plugs done.")


# =============================================================================
# 2. CONSUMPTION AGGREGATES
# =============================================================================

def agg_consumption(conn):
    log.info("  Aggregating consumption...")
    with conn.cursor() as cur:

        # --- HOURLY ---
        cur.execute("""
            INSERT INTO gold.agg_consumption_hourly (
                id_building, hour_ts,
                avg_total_power_w, min_total_power_w, max_total_power_w,
                avg_power1_w, avg_power2_w, avg_power3_w,
                avg_current1_a, avg_current2_a, avg_current3_a,
                avg_voltage1_v, avg_voltage2_v, avg_voltage3_v,
                avg_pf1, avg_pf2, avg_pf3,
                estimated_energy_wh, sample_count
            )
            SELECT
                c.id_building,
                DATE_TRUNC('hour', e.event_datetime)    AS hour_ts,
                AVG(c.total_power_w),
                MIN(c.total_power_w),
                MAX(c.total_power_w),
                AVG(c.power1_w), AVG(c.power2_w), AVG(c.power3_w),
                AVG(c.current1_a), AVG(c.current2_a), AVG(c.current3_a),
                AVG(c.voltage1_v), AVG(c.voltage2_v), AVG(c.voltage3_v),
                AVG(c.pf1), AVG(c.pf2), AVG(c.pf3),
                AVG(c.total_power_w) * 1.0,             -- Wh estimate for 1 hour
                COUNT(*)
            FROM silver.iot_consumption c
            JOIN silver.iot_events e ON e.event_id = c.event_id
            GROUP BY c.id_building, DATE_TRUNC('hour', e.event_datetime)
            ON CONFLICT (id_building, hour_ts) DO UPDATE SET
                avg_total_power_w   = EXCLUDED.avg_total_power_w,
                min_total_power_w   = EXCLUDED.min_total_power_w,
                max_total_power_w   = EXCLUDED.max_total_power_w,
                avg_power1_w        = EXCLUDED.avg_power1_w,
                avg_power2_w        = EXCLUDED.avg_power2_w,
                avg_power3_w        = EXCLUDED.avg_power3_w,
                avg_current1_a      = EXCLUDED.avg_current1_a,
                avg_current2_a      = EXCLUDED.avg_current2_a,
                avg_current3_a      = EXCLUDED.avg_current3_a,
                avg_voltage1_v      = EXCLUDED.avg_voltage1_v,
                avg_voltage2_v      = EXCLUDED.avg_voltage2_v,
                avg_voltage3_v      = EXCLUDED.avg_voltage3_v,
                avg_pf1             = EXCLUDED.avg_pf1,
                avg_pf2             = EXCLUDED.avg_pf2,
                avg_pf3             = EXCLUDED.avg_pf3,
                estimated_energy_wh = EXCLUDED.estimated_energy_wh,
                sample_count        = EXCLUDED.sample_count,
                ingested_at         = CURRENT_TIMESTAMP
        """)

        # --- DAILY (sum hourly energy estimates) ---
        cur.execute("""
            INSERT INTO gold.agg_consumption_daily (
                id_building, day_date,
                avg_total_power_w, min_total_power_w, max_total_power_w, peak_total_power_w,
                avg_power1_w, avg_power2_w, avg_power3_w,
                avg_voltage1_v, avg_voltage2_v, avg_voltage3_v,
                avg_pf1, avg_pf2, avg_pf3,
                estimated_energy_wh, sample_count
            )
            SELECT
                c.id_building,
                DATE_TRUNC('day', e.event_datetime)::DATE   AS day_date,
                AVG(c.total_power_w),
                MIN(c.total_power_w),
                MAX(c.total_power_w),
                MAX(c.total_power_w),
                AVG(c.power1_w), AVG(c.power2_w), AVG(c.power3_w),
                AVG(c.voltage1_v), AVG(c.voltage2_v), AVG(c.voltage3_v),
                AVG(c.pf1), AVG(c.pf2), AVG(c.pf3),
                -- Sum of hourly estimates (avg_power * 1h) grouped per day
                SUM(c.total_power_w) / COUNT(DISTINCT DATE_TRUNC('hour', e.event_datetime)),
                COUNT(*)
            FROM silver.iot_consumption c
            JOIN silver.iot_events e ON e.event_id = c.event_id
            GROUP BY c.id_building, DATE_TRUNC('day', e.event_datetime)::DATE
            ON CONFLICT (id_building, day_date) DO UPDATE SET
                avg_total_power_w   = EXCLUDED.avg_total_power_w,
                min_total_power_w   = EXCLUDED.min_total_power_w,
                max_total_power_w   = EXCLUDED.max_total_power_w,
                peak_total_power_w  = EXCLUDED.peak_total_power_w,
                avg_power1_w        = EXCLUDED.avg_power1_w,
                avg_power2_w        = EXCLUDED.avg_power2_w,
                avg_power3_w        = EXCLUDED.avg_power3_w,
                avg_voltage1_v      = EXCLUDED.avg_voltage1_v,
                avg_voltage2_v      = EXCLUDED.avg_voltage2_v,
                avg_voltage3_v      = EXCLUDED.avg_voltage3_v,
                avg_pf1             = EXCLUDED.avg_pf1,
                avg_pf2             = EXCLUDED.avg_pf2,
                avg_pf3             = EXCLUDED.avg_pf3,
                estimated_energy_wh = EXCLUDED.estimated_energy_wh,
                sample_count        = EXCLUDED.sample_count,
                ingested_at         = CURRENT_TIMESTAMP
        """)

    conn.commit()
    log.info("  Consumption done.")


# =============================================================================
# 3. METEO INDOOR AGGREGATES
# =============================================================================

def agg_meteo_indoor(conn):
    log.info("  Aggregating indoor meteo...")
    with conn.cursor() as cur:

        # --- HOURLY ---
        cur.execute("""
            INSERT INTO gold.agg_meteo_indoor_hourly (
                id_building, station, hour_ts,
                avg_temperature_c, min_temperature_c, max_temperature_c,
                avg_humidity_pct, avg_co2_ppm, avg_noise_db,
                avg_pressure_hpa, sample_count
            )
            SELECT
                m.id_building,
                m.station,
                DATE_TRUNC('hour', e.event_datetime)    AS hour_ts,
                AVG(m.temperature_c),
                MIN(m.temperature_c),
                MAX(m.temperature_c),
                AVG(m.humidity_pct),
                AVG(m.co2_ppm),
                AVG(m.noise_db),
                AVG(m.pressure_hpa),
                COUNT(*)
            FROM silver.iot_meteo_indoor m
            JOIN silver.iot_events e ON e.event_id = m.event_id
            GROUP BY m.id_building, m.station, DATE_TRUNC('hour', e.event_datetime)
            ON CONFLICT (id_building, station, hour_ts) DO UPDATE SET
                avg_temperature_c = EXCLUDED.avg_temperature_c,
                min_temperature_c = EXCLUDED.min_temperature_c,
                max_temperature_c = EXCLUDED.max_temperature_c,
                avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
                avg_co2_ppm       = EXCLUDED.avg_co2_ppm,
                avg_noise_db      = EXCLUDED.avg_noise_db,
                avg_pressure_hpa  = EXCLUDED.avg_pressure_hpa,
                sample_count      = EXCLUDED.sample_count,
                ingested_at       = CURRENT_TIMESTAMP
        """)

        # --- DAILY ---
        cur.execute("""
            INSERT INTO gold.agg_meteo_indoor_daily (
                id_building, station, day_date,
                avg_temperature_c, min_temperature_c, max_temperature_c,
                avg_humidity_pct, min_humidity_pct, max_humidity_pct,
                avg_co2_ppm, max_co2_ppm, avg_noise_db,
                avg_pressure_hpa, sample_count
            )
            SELECT
                m.id_building,
                m.station,
                DATE_TRUNC('day', e.event_datetime)::DATE   AS day_date,
                AVG(m.temperature_c),
                MIN(m.temperature_c),
                MAX(m.temperature_c),
                AVG(m.humidity_pct),
                MIN(m.humidity_pct),
                MAX(m.humidity_pct),
                AVG(m.co2_ppm),
                MAX(m.co2_ppm),
                AVG(m.noise_db),
                AVG(m.pressure_hpa),
                COUNT(*)
            FROM silver.iot_meteo_indoor m
            JOIN silver.iot_events e ON e.event_id = m.event_id
            GROUP BY m.id_building, m.station, DATE_TRUNC('day', e.event_datetime)::DATE
            ON CONFLICT (id_building, station, day_date) DO UPDATE SET
                avg_temperature_c = EXCLUDED.avg_temperature_c,
                min_temperature_c = EXCLUDED.min_temperature_c,
                max_temperature_c = EXCLUDED.max_temperature_c,
                avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
                min_humidity_pct  = EXCLUDED.min_humidity_pct,
                max_humidity_pct  = EXCLUDED.max_humidity_pct,
                avg_co2_ppm       = EXCLUDED.avg_co2_ppm,
                max_co2_ppm       = EXCLUDED.max_co2_ppm,
                avg_noise_db      = EXCLUDED.avg_noise_db,
                avg_pressure_hpa  = EXCLUDED.avg_pressure_hpa,
                sample_count      = EXCLUDED.sample_count,
                ingested_at       = CURRENT_TIMESTAMP
        """)

    conn.commit()
    log.info("  Indoor meteo done.")


# =============================================================================
# 4. HUMIDITY AGGREGATES
# =============================================================================

def agg_humidity(conn):
    log.info("  Aggregating humidity sensors...")
    with conn.cursor() as cur:

        # --- HOURLY ---
        cur.execute("""
            INSERT INTO gold.agg_humidity_hourly (
                id_building, room, hour_ts,
                avg_temperature_c, avg_humidity_pct,
                min_humidity_pct, max_humidity_pct, sample_count
            )
            SELECT
                h.id_building,
                h.room,
                DATE_TRUNC('hour', e.event_datetime)    AS hour_ts,
                AVG(h.temperature_c),
                AVG(h.humidity_pct),
                MIN(h.humidity_pct),
                MAX(h.humidity_pct),
                COUNT(*)
            FROM silver.iot_humidities h
            JOIN silver.iot_events e ON e.event_id = h.event_id
            GROUP BY h.id_building, h.room, DATE_TRUNC('hour', e.event_datetime)
            ON CONFLICT (id_building, room, hour_ts) DO UPDATE SET
                avg_temperature_c = EXCLUDED.avg_temperature_c,
                avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
                min_humidity_pct  = EXCLUDED.min_humidity_pct,
                max_humidity_pct  = EXCLUDED.max_humidity_pct,
                sample_count      = EXCLUDED.sample_count,
                ingested_at       = CURRENT_TIMESTAMP
        """)

        # --- DAILY ---
        cur.execute("""
            INSERT INTO gold.agg_humidity_daily (
                id_building, room, day_date,
                avg_temperature_c, avg_humidity_pct,
                min_humidity_pct, max_humidity_pct, sample_count
            )
            SELECT
                h.id_building,
                h.room,
                DATE_TRUNC('day', e.event_datetime)::DATE   AS day_date,
                AVG(h.temperature_c),
                AVG(h.humidity_pct),
                MIN(h.humidity_pct),
                MAX(h.humidity_pct),
                COUNT(*)
            FROM silver.iot_humidities h
            JOIN silver.iot_events e ON e.event_id = h.event_id
            GROUP BY h.id_building, h.room, DATE_TRUNC('day', e.event_datetime)::DATE
            ON CONFLICT (id_building, room, day_date) DO UPDATE SET
                avg_temperature_c = EXCLUDED.avg_temperature_c,
                avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
                min_humidity_pct  = EXCLUDED.min_humidity_pct,
                max_humidity_pct  = EXCLUDED.max_humidity_pct,
                sample_count      = EXCLUDED.sample_count,
                ingested_at       = CURRENT_TIMESTAMP
        """)

    conn.commit()
    log.info("  Humidity done.")


# =============================================================================
# 5. OCCUPANCY AGGREGATES
# =============================================================================

def agg_occupancy(conn):
    log.info("  Aggregating occupancy...")
    with conn.cursor() as cur:

        # --- HOURLY ---
        cur.execute("""
            INSERT INTO gold.agg_occupancy_hourly (
                id_building, hour_ts,
                any_motion_detected, rooms_with_motion, avg_light_lux,
                avg_doors_open_count, avg_windows_open_count,
                max_doors_open_count, max_windows_open_count,
                sample_count
            )
            WITH motion_agg AS (
                SELECT
                    mo.id_building,
                    DATE_TRUNC('hour', e.event_datetime)    AS hour_ts,
                    BOOL_OR(mo.motion_detected)             AS any_motion,
                    COUNT(DISTINCT CASE WHEN mo.motion_detected THEN mo.room END) AS rooms_with_motion,
                    AVG(mo.light_lux)                       AS avg_light,
                    COUNT(DISTINCT e.event_id)              AS sample_count
                FROM silver.iot_motions mo
                JOIN silver.iot_events e ON e.event_id = mo.event_id
                GROUP BY mo.id_building, DATE_TRUNC('hour', e.event_datetime)
            ),
            dw_agg AS (
                SELECT
                    dw.id_building,
                    DATE_TRUNC('hour', e.event_datetime)    AS hour_ts,
                    AVG(CASE WHEN dw.sensor_type = 'Door'   AND dw.is_open THEN 1.0 ELSE 0.0 END) AS avg_doors,
                    AVG(CASE WHEN dw.sensor_type = 'Window' AND dw.is_open THEN 1.0 ELSE 0.0 END) AS avg_windows,
                    MAX(CASE WHEN dw.sensor_type = 'Door'   AND dw.is_open THEN 1   ELSE 0   END) AS max_doors,
                    MAX(CASE WHEN dw.sensor_type = 'Window' AND dw.is_open THEN 1   ELSE 0   END) AS max_windows
                FROM silver.iot_doors_windows dw
                JOIN silver.iot_events e ON e.event_id = dw.event_id
                GROUP BY dw.id_building, DATE_TRUNC('hour', e.event_datetime)
            )
            SELECT
                m.id_building,
                m.hour_ts,
                m.any_motion,
                m.rooms_with_motion,
                m.avg_light,
                d.avg_doors,
                d.avg_windows,
                d.max_doors,
                d.max_windows,
                m.sample_count
            FROM motion_agg m
            LEFT JOIN dw_agg d ON d.id_building = m.id_building AND d.hour_ts = m.hour_ts
            ON CONFLICT (id_building, hour_ts) DO UPDATE SET
                any_motion_detected    = EXCLUDED.any_motion_detected,
                rooms_with_motion      = EXCLUDED.rooms_with_motion,
                avg_light_lux          = EXCLUDED.avg_light_lux,
                avg_doors_open_count   = EXCLUDED.avg_doors_open_count,
                avg_windows_open_count = EXCLUDED.avg_windows_open_count,
                max_doors_open_count   = EXCLUDED.max_doors_open_count,
                max_windows_open_count = EXCLUDED.max_windows_open_count,
                sample_count           = EXCLUDED.sample_count,
                ingested_at            = CURRENT_TIMESTAMP
        """)

        # --- DAILY ---
        cur.execute("""
            INSERT INTO gold.agg_occupancy_daily (
                id_building, day_date,
                hours_with_motion, total_motion_events, pct_hours_occupied,
                avg_doors_open_count, avg_windows_open_count,
                max_doors_open_count, max_windows_open_count,
                sample_count
            )
            WITH motion_agg AS (
                SELECT
                    mo.id_building,
                    DATE_TRUNC('day', e.event_datetime)::DATE               AS day_date,
                    COUNT(DISTINCT CASE WHEN mo.motion_detected
                        THEN DATE_TRUNC('hour', e.event_datetime) END)      AS hours_with_motion,
                    COUNT(CASE WHEN mo.motion_detected THEN 1 END)          AS total_motion_events,
                    COUNT(DISTINCT e.event_id)                              AS sample_count
                FROM silver.iot_motions mo
                JOIN silver.iot_events e ON e.event_id = mo.event_id
                GROUP BY mo.id_building, DATE_TRUNC('day', e.event_datetime)::DATE
            ),
            dw_agg AS (
                SELECT
                    dw.id_building,
                    DATE_TRUNC('day', e.event_datetime)::DATE   AS day_date,
                    AVG(CASE WHEN dw.sensor_type = 'Door'   AND dw.is_open THEN 1.0 ELSE 0.0 END) AS avg_doors,
                    AVG(CASE WHEN dw.sensor_type = 'Window' AND dw.is_open THEN 1.0 ELSE 0.0 END) AS avg_windows,
                    MAX(CASE WHEN dw.sensor_type = 'Door'   AND dw.is_open THEN 1   ELSE 0   END) AS max_doors,
                    MAX(CASE WHEN dw.sensor_type = 'Window' AND dw.is_open THEN 1   ELSE 0   END) AS max_windows
                FROM silver.iot_doors_windows dw
                JOIN silver.iot_events e ON e.event_id = dw.event_id
                GROUP BY dw.id_building, DATE_TRUNC('day', e.event_datetime)::DATE
            )
            SELECT
                m.id_building,
                m.day_date,
                m.hours_with_motion,
                m.total_motion_events,
                m.hours_with_motion::DOUBLE PRECISION / 24.0,
                d.avg_doors,
                d.avg_windows,
                d.max_doors,
                d.max_windows,
                m.sample_count
            FROM motion_agg m
            LEFT JOIN dw_agg d ON d.id_building = m.id_building AND d.day_date = m.day_date
            ON CONFLICT (id_building, day_date) DO UPDATE SET
                hours_with_motion      = EXCLUDED.hours_with_motion,
                total_motion_events    = EXCLUDED.total_motion_events,
                pct_hours_occupied     = EXCLUDED.pct_hours_occupied,
                avg_doors_open_count   = EXCLUDED.avg_doors_open_count,
                avg_windows_open_count = EXCLUDED.avg_windows_open_count,
                max_doors_open_count   = EXCLUDED.max_doors_open_count,
                max_windows_open_count = EXCLUDED.max_windows_open_count,
                sample_count           = EXCLUDED.sample_count,
                ingested_at            = CURRENT_TIMESTAMP
        """)

    conn.commit()
    log.info("  Occupancy done.")


# =============================================================================
# 6. ANOMALY FLAGS
# =============================================================================

def agg_anomalies(conn):
    log.info("  Computing anomaly flags...")
    with conn.cursor() as cur:

        # Fetch all events not yet in anomaly_flags
        cur.execute("""
            SELECT e.event_id, e.id_building, e.event_datetime
            FROM silver.iot_events e
            WHERE NOT EXISTS (
                SELECT 1 FROM gold.anomaly_flags af WHERE af.event_id = e.event_id
            )
            ORDER BY e.event_datetime
        """)
        events = cur.fetchall()
        log.info(f"    {len(events)} new events to flag")

        for event_id, id_building, event_datetime in events:
            flags = {
                "event_id":                  event_id,
                "id_building":               id_building,
                "event_datetime":            event_datetime,
                "overpower_detected":        False,
                "overtemperature_detected":  False,
                "high_plug_power_room":      None,
                "total_power_spike":         False,
                "phase_imbalance":           False,
                "high_co2_detected":         False,
                "high_co2_station":          None,
                "max_co2_ppm":               None,
                "high_humidity_detected":    False,
                "high_humidity_room":        None,
                "low_battery_detected":      False,
                "low_battery_device":        None,
            }

            # -- Plug anomalies --
            cur.execute("""
                SELECT room, overpower, overtemperature
                FROM silver.iot_plugs
                WHERE event_id = %s AND (overpower > 0 OR overtemperature = TRUE)
            """, (event_id,))
            plug_issues = cur.fetchall()
            if plug_issues:
                for room, overpower, overtemp in plug_issues:
                    if overpower and overpower > 0:
                        flags["overpower_detected"]    = True
                        flags["high_plug_power_room"]  = room
                    if overtemp:
                        flags["overtemperature_detected"] = True

            # -- Consumption anomalies --
            cur.execute("""
                SELECT total_power_w, power1_w, power2_w, power3_w
                FROM silver.iot_consumption
                WHERE event_id = %s
            """, (event_id,))
            cons = cur.fetchone()
            if cons:
                total, p1, p2, p3 = cons
                # Phase imbalance: max phase > PHASE_IMBALANCE_RATIO * min phase
                phases = [x for x in [p1, p2, p3] if x is not None and x > 0]
                if len(phases) >= 2:
                    if max(phases) > PHASE_IMBALANCE_RATIO * min(phases):
                        flags["phase_imbalance"] = True

                # Power spike: compare to daily average for this building
                if total is not None:
                    cur.execute("""
                        SELECT AVG(c.total_power_w)
                        FROM silver.iot_consumption c
                        JOIN silver.iot_events e ON e.event_id = c.event_id
                        WHERE c.id_building = %s
                          AND DATE_TRUNC('day', e.event_datetime) =
                              DATE_TRUNC('day', %s::TIMESTAMP)
                    """, (id_building, event_datetime))
                    avg_row = cur.fetchone()
                    if avg_row and avg_row[0] and total > 2 * avg_row[0]:
                        flags["total_power_spike"] = True

            # -- CO2 anomalies --
            cur.execute("""
                SELECT station, co2_ppm
                FROM silver.iot_meteo_indoor
                WHERE event_id = %s AND co2_ppm > %s
                ORDER BY co2_ppm DESC
                LIMIT 1
            """, (event_id, CO2_HIGH_THRESHOLD))
            co2_row = cur.fetchone()
            if co2_row:
                flags["high_co2_detected"] = True
                flags["high_co2_station"]  = co2_row[0]
                flags["max_co2_ppm"]       = co2_row[1]

            # -- Humidity anomalies --
            cur.execute("""
                SELECT room, humidity_pct
                FROM silver.iot_humidities
                WHERE event_id = %s AND humidity_pct > %s
                ORDER BY humidity_pct DESC
                LIMIT 1
            """, (event_id, HUMIDITY_HIGH_THRESHOLD))
            hum_row = cur.fetchone()
            if hum_row:
                flags["high_humidity_detected"] = True
                flags["high_humidity_room"]      = hum_row[0]

            # -- Battery anomalies (doors/windows sensors + meteo stations) --
            low_battery_devices = []

            cur.execute("""
                SELECT room, sensor_type, battery_pct
                FROM silver.iot_doors_windows
                WHERE event_id = %s AND battery_pct < %s
            """, (event_id, BATTERY_LOW_THRESHOLD))
            for room, stype, pct in cur.fetchall():
                low_battery_devices.append(f"{room}_{stype}({pct}%)")

            cur.execute("""
                SELECT station, battery_pct
                FROM silver.iot_meteo_indoor
                WHERE event_id = %s AND battery_pct IS NOT NULL AND battery_pct < %s
            """, (event_id, BATTERY_LOW_THRESHOLD))
            for station, pct in cur.fetchall():
                low_battery_devices.append(f"meteo_{station}({pct}%)")

            if low_battery_devices:
                flags["low_battery_detected"] = True
                flags["low_battery_device"]   = ", ".join(low_battery_devices)

            # -- Insert flag row --
            cur.execute("""
                INSERT INTO gold.anomaly_flags (
                    event_id, id_building, event_datetime,
                    overpower_detected, overtemperature_detected, high_plug_power_room,
                    total_power_spike, phase_imbalance,
                    high_co2_detected, high_co2_station, max_co2_ppm,
                    high_humidity_detected, high_humidity_room,
                    low_battery_detected, low_battery_device
                ) VALUES (
                    %(event_id)s, %(id_building)s, %(event_datetime)s,
                    %(overpower_detected)s, %(overtemperature_detected)s, %(high_plug_power_room)s,
                    %(total_power_spike)s, %(phase_imbalance)s,
                    %(high_co2_detected)s, %(high_co2_station)s, %(max_co2_ppm)s,
                    %(high_humidity_detected)s, %(high_humidity_room)s,
                    %(low_battery_detected)s, %(low_battery_device)s
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    overpower_detected       = EXCLUDED.overpower_detected,
                    overtemperature_detected = EXCLUDED.overtemperature_detected,
                    high_plug_power_room     = EXCLUDED.high_plug_power_room,
                    total_power_spike        = EXCLUDED.total_power_spike,
                    phase_imbalance          = EXCLUDED.phase_imbalance,
                    high_co2_detected        = EXCLUDED.high_co2_detected,
                    high_co2_station         = EXCLUDED.high_co2_station,
                    max_co2_ppm              = EXCLUDED.max_co2_ppm,
                    high_humidity_detected   = EXCLUDED.high_humidity_detected,
                    high_humidity_room       = EXCLUDED.high_humidity_room,
                    low_battery_detected     = EXCLUDED.low_battery_detected,
                    low_battery_device       = EXCLUDED.low_battery_device,
                    ingested_at              = CURRENT_TIMESTAMP
            """, flags)

        conn.commit()
    log.info("  Anomalies done.")


# =============================================================================
# 7. SOLAR PREDICTION AGGREGATES
# =============================================================================

def agg_solar(conn):
    log.info("  Aggregating solar predictions...")
    with conn.cursor() as cur:

        # --- HOURLY ---
        cur.execute("""
            INSERT INTO gold.agg_solar_prediction_hourly (
                site, hour_ts,
                avg_irradiance_wm2, max_irradiance_wm2,
                is_missing, sample_count
            )
            SELECT
                site,
                DATE_TRUNC('hour', prediction_time)     AS hour_ts,
                AVG(CASE WHEN value_wm2 > -9999 THEN value_wm2 END),
                MAX(CASE WHEN value_wm2 > -9999 THEN value_wm2 END),
                BOOL_AND(value_wm2 <= -9999),
                COUNT(*)
            FROM silver.meteo_solar_prediction
            GROUP BY site, DATE_TRUNC('hour', prediction_time)
            ON CONFLICT (site, hour_ts) DO UPDATE SET
                avg_irradiance_wm2 = EXCLUDED.avg_irradiance_wm2,
                max_irradiance_wm2 = EXCLUDED.max_irradiance_wm2,
                is_missing         = EXCLUDED.is_missing,
                sample_count       = EXCLUDED.sample_count,
                ingested_at        = CURRENT_TIMESTAMP
        """)

        # --- DAILY ---
        cur.execute("""
            INSERT INTO gold.agg_solar_prediction_daily (
                site, day_date,
                avg_irradiance_wm2, max_irradiance_wm2,
                peak_hour, is_missing, sample_count
            )
            WITH ranked AS (
                SELECT
                    site,
                    DATE_TRUNC('day', prediction_time)::DATE    AS day_date,
                    EXTRACT(HOUR FROM prediction_time)::INT     AS hr,
                    value_wm2,
                    RANK() OVER (
                        PARTITION BY site, DATE_TRUNC('day', prediction_time)
                        ORDER BY CASE WHEN value_wm2 > -9999 THEN value_wm2 END DESC NULLS LAST
                    ) AS rnk
                FROM silver.meteo_solar_prediction
            )
            SELECT
                site,
                day_date,
                AVG(CASE WHEN value_wm2 > -9999 THEN value_wm2 END),
                MAX(CASE WHEN value_wm2 > -9999 THEN value_wm2 END),
                MAX(CASE WHEN rnk = 1 THEN hr END),
                BOOL_AND(value_wm2 <= -9999),
                COUNT(*)
            FROM ranked
            GROUP BY site, day_date
            ON CONFLICT (site, day_date) DO UPDATE SET
                avg_irradiance_wm2 = EXCLUDED.avg_irradiance_wm2,
                max_irradiance_wm2 = EXCLUDED.max_irradiance_wm2,
                peak_hour          = EXCLUDED.peak_hour,
                is_missing         = EXCLUDED.is_missing,
                sample_count       = EXCLUDED.sample_count,
                ingested_at        = CURRENT_TIMESTAMP
        """)

    conn.commit()
    log.info("  Solar done.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    log.info("========================================")
    log.info("  ETL Gold Layer - Start")
    log.info("========================================\n")

    start = datetime.now()

    try:
        conn = get_connection()
        log.info("Database connection established.\n")
    except Exception as e:
        log.critical(f"Cannot connect to database: {e}")
        return

    try:
        log.info("=== Step 1/7: Plugs ===")
        agg_plugs(conn)

        log.info("=== Step 2/7: Consumption ===")
        agg_consumption(conn)

        log.info("=== Step 3/7: Indoor Meteo ===")
        agg_meteo_indoor(conn)

        log.info("=== Step 4/7: Humidity ===")
        agg_humidity(conn)

        log.info("=== Step 5/7: Occupancy ===")
        agg_occupancy(conn)

        log.info("=== Step 6/7: Anomaly Flags ===")
        agg_anomalies(conn)

        log.info("=== Step 7/7: Solar Predictions ===")
        agg_solar(conn)

    except Exception as e:
        log.critical(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        conn.close()

    elapsed = datetime.now() - start
    log.info("\n========================================")
    log.info(f"  ETL Gold Complete - elapsed: {elapsed}")
    log.info("========================================")


if __name__ == "__main__":
    main()
