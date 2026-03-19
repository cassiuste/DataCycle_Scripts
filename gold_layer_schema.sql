-- =============================================================================
-- GOLD LAYER SCHEMA
-- Aggregated, normalized, ML-ready tables built from silver layer
-- Granularity: hourly and daily, per building
-- =============================================================================


-- =============================================================================
-- 1. ENERGY AGGREGATES – plugs (per room)
--    Source: silver.iot_plugs
-- =============================================================================

CREATE TABLE gold.agg_plugs_hourly (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,
    hour_ts             TIMESTAMP       NOT NULL,   -- truncated to the hour (e.g. 2023-05-31 17:00)

    avg_power_w         DOUBLE PRECISION,
    min_power_w         DOUBLE PRECISION,
    max_power_w         DOUBLE PRECISION,
    avg_temperature_c   DOUBLE PRECISION,
    pct_switch_on       DOUBLE PRECISION,           -- % of readings where plug was ON (0-1)
    sample_count        INT,                        -- number of raw readings in this bucket

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, room, hour_ts)
);

CREATE TABLE gold.agg_plugs_daily (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,
    day_date            DATE            NOT NULL,

    avg_power_w         DOUBLE PRECISION,
    min_power_w         DOUBLE PRECISION,
    max_power_w         DOUBLE PRECISION,
    avg_temperature_c   DOUBLE PRECISION,
    pct_switch_on       DOUBLE PRECISION,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, room, day_date)
);


-- =============================================================================
-- 2. ENERGY KPIs - whole-house consumption (per phase + total)
--    Source: silver.iot_consumption
-- =============================================================================

CREATE TABLE gold.agg_consumption_hourly (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    hour_ts             TIMESTAMP       NOT NULL,

    avg_total_power_w   DOUBLE PRECISION,
    min_total_power_w   DOUBLE PRECISION,
    max_total_power_w   DOUBLE PRECISION,

    avg_power1_w        DOUBLE PRECISION,
    avg_power2_w        DOUBLE PRECISION,
    avg_power3_w        DOUBLE PRECISION,

    avg_current1_a      DOUBLE PRECISION,
    avg_current2_a      DOUBLE PRECISION,
    avg_current3_a      DOUBLE PRECISION,

    avg_voltage1_v      DOUBLE PRECISION,
    avg_voltage2_v      DOUBLE PRECISION,
    avg_voltage3_v      DOUBLE PRECISION,

    avg_pf1             DOUBLE PRECISION,
    avg_pf2             DOUBLE PRECISION,
    avg_pf3             DOUBLE PRECISION,

    estimated_energy_wh DOUBLE PRECISION,           -- avg_total_power_w * 1h

    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, hour_ts)
);

CREATE TABLE gold.agg_consumption_daily (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    day_date            DATE            NOT NULL,

    avg_total_power_w   DOUBLE PRECISION,
    min_total_power_w   DOUBLE PRECISION,
    max_total_power_w   DOUBLE PRECISION,
    peak_total_power_w  DOUBLE PRECISION,

    avg_power1_w        DOUBLE PRECISION,
    avg_power2_w        DOUBLE PRECISION,
    avg_power3_w        DOUBLE PRECISION,

    avg_voltage1_v      DOUBLE PRECISION,
    avg_voltage2_v      DOUBLE PRECISION,
    avg_voltage3_v      DOUBLE PRECISION,

    avg_pf1             DOUBLE PRECISION,
    avg_pf2             DOUBLE PRECISION,
    avg_pf3             DOUBLE PRECISION,

    estimated_energy_wh DOUBLE PRECISION,           -- sum of hourly estimates

    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, day_date)
);


-- =============================================================================
-- 3. CLIMATE AGGREGATES - indoor meteo (per station)
--    Source: silver.iot_meteo_indoor
-- =============================================================================

CREATE TABLE gold.agg_meteo_indoor_hourly (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    station             VARCHAR(100)    NOT NULL,
    hour_ts             TIMESTAMP       NOT NULL,

    avg_temperature_c   DOUBLE PRECISION,
    min_temperature_c   DOUBLE PRECISION,
    max_temperature_c   DOUBLE PRECISION,
    avg_humidity_pct    DOUBLE PRECISION,
    avg_co2_ppm         DOUBLE PRECISION,
    avg_noise_db        DOUBLE PRECISION,
    avg_pressure_hpa    DOUBLE PRECISION,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, station, hour_ts)
);

CREATE TABLE gold.agg_meteo_indoor_daily (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    station             VARCHAR(100)    NOT NULL,
    day_date            DATE            NOT NULL,

    avg_temperature_c   DOUBLE PRECISION,
    min_temperature_c   DOUBLE PRECISION,
    max_temperature_c   DOUBLE PRECISION,
    avg_humidity_pct    DOUBLE PRECISION,
    min_humidity_pct    DOUBLE PRECISION,
    max_humidity_pct    DOUBLE PRECISION,
    avg_co2_ppm         DOUBLE PRECISION,
    max_co2_ppm         DOUBLE PRECISION,
    avg_noise_db        DOUBLE PRECISION,
    avg_pressure_hpa    DOUBLE PRECISION,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, station, day_date)
);


-- =============================================================================
-- 4. HUMIDITY AGGREGATES (laundry, bathroom)
--    Source: silver.iot_humidities
-- =============================================================================

CREATE TABLE gold.agg_humidity_hourly (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,
    hour_ts             TIMESTAMP       NOT NULL,

    avg_temperature_c   DOUBLE PRECISION,
    avg_humidity_pct    DOUBLE PRECISION,
    min_humidity_pct    DOUBLE PRECISION,
    max_humidity_pct    DOUBLE PRECISION,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, room, hour_ts)
);

CREATE TABLE gold.agg_humidity_daily (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,
    day_date            DATE            NOT NULL,

    avg_temperature_c   DOUBLE PRECISION,
    avg_humidity_pct    DOUBLE PRECISION,
    min_humidity_pct    DOUBLE PRECISION,
    max_humidity_pct    DOUBLE PRECISION,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, room, day_date)
);


-- =============================================================================
-- 5. OCCUPANCY INDICATORS
--    Source: silver.iot_motions + silver.iot_doors_windows
-- =============================================================================

CREATE TABLE gold.agg_occupancy_hourly (
    id                      BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building             INT             NOT NULL,
    hour_ts                 TIMESTAMP       NOT NULL,

    any_motion_detected     BOOLEAN,
    rooms_with_motion       INT,
    avg_light_lux           DOUBLE PRECISION,

    avg_doors_open_count    DOUBLE PRECISION,
    avg_windows_open_count  DOUBLE PRECISION,
    max_doors_open_count    INT,
    max_windows_open_count  INT,

    sample_count            INT,

    ingested_at             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, hour_ts)
);

CREATE TABLE gold.agg_occupancy_daily (
    id                      BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building             INT             NOT NULL,
    day_date                DATE            NOT NULL,

    hours_with_motion       INT,
    total_motion_events     INT,
    pct_hours_occupied      DOUBLE PRECISION,       -- hours_with_motion / 24

    avg_doors_open_count    DOUBLE PRECISION,
    avg_windows_open_count  DOUBLE PRECISION,
    max_doors_open_count    INT,
    max_windows_open_count  INT,

    sample_count            INT,

    ingested_at             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (id_building, day_date)
);


-- =============================================================================
-- 6. ANOMALY FLAGS
--    Source: all silver IoT tables
--    One row per event_id
-- =============================================================================

CREATE TABLE gold.anomaly_flags (
    id                          BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id                    BIGINT          NOT NULL,
    id_building                 INT             NOT NULL,
    event_datetime              TIMESTAMP       NOT NULL,

    -- Plug anomalies
    overpower_detected          BOOLEAN         DEFAULT FALSE,
    overtemperature_detected    BOOLEAN         DEFAULT FALSE,
    high_plug_power_room        VARCHAR(100),

    -- Consumption anomalies
    total_power_spike           BOOLEAN         DEFAULT FALSE,
    phase_imbalance             BOOLEAN         DEFAULT FALSE,

    -- Climate anomalies
    high_co2_detected           BOOLEAN         DEFAULT FALSE,
    high_co2_station            VARCHAR(100),
    max_co2_ppm                 INT,

    high_humidity_detected      BOOLEAN         DEFAULT FALSE,
    high_humidity_room          VARCHAR(100),

    -- Battery anomalies
    low_battery_detected        BOOLEAN         DEFAULT FALSE,
    low_battery_device          VARCHAR(200),   -- comma-separated list

    ingested_at                 TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (event_id)
);


-- =============================================================================
-- 7. SOLAR PREDICTION AGGREGATES
--    Source: silver.meteo_solar_prediction
-- =============================================================================

CREATE TABLE gold.agg_solar_prediction_hourly (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    site                VARCHAR(200)    NOT NULL,
    hour_ts             TIMESTAMP       NOT NULL,

    avg_irradiance_wm2  DOUBLE PRECISION,
    max_irradiance_wm2  DOUBLE PRECISION,
    is_missing          BOOLEAN,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (site, hour_ts)
);

CREATE TABLE gold.agg_solar_prediction_daily (
    id                  BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    site                VARCHAR(200)    NOT NULL,
    day_date            DATE            NOT NULL,

    avg_irradiance_wm2  DOUBLE PRECISION,
    max_irradiance_wm2  DOUBLE PRECISION,
    peak_hour           INT,                        -- hour of day (0-23) with max irradiance
    is_missing          BOOLEAN,
    sample_count        INT,

    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (site, day_date)
);


-- =============================================================================
-- 8. INDEXES
-- =============================================================================

CREATE INDEX idx_gold_plugs_h_building     ON gold.agg_plugs_hourly(id_building, hour_ts);
CREATE INDEX idx_gold_plugs_d_building     ON gold.agg_plugs_daily(id_building, day_date);

CREATE INDEX idx_gold_cons_h_building      ON gold.agg_consumption_hourly(id_building, hour_ts);
CREATE INDEX idx_gold_cons_d_building      ON gold.agg_consumption_daily(id_building, day_date);

CREATE INDEX idx_gold_meteo_h_building     ON gold.agg_meteo_indoor_hourly(id_building, station, hour_ts);
CREATE INDEX idx_gold_meteo_d_building     ON gold.agg_meteo_indoor_daily(id_building, station, day_date);

CREATE INDEX idx_gold_hum_h_building       ON gold.agg_humidity_hourly(id_building, room, hour_ts);
CREATE INDEX idx_gold_hum_d_building       ON gold.agg_humidity_daily(id_building, room, day_date);

CREATE INDEX idx_gold_occ_h_building       ON gold.agg_occupancy_hourly(id_building, hour_ts);
CREATE INDEX idx_gold_occ_d_building       ON gold.agg_occupancy_daily(id_building, day_date);

CREATE INDEX idx_gold_anomaly_building     ON gold.anomaly_flags(id_building, event_datetime);
CREATE INDEX idx_gold_anomaly_event        ON gold.anomaly_flags(event_id);

CREATE INDEX idx_gold_solar_h_site         ON gold.agg_solar_prediction_hourly(site, hour_ts);
CREATE INDEX idx_gold_solar_d_site         ON gold.agg_solar_prediction_daily(site, day_date);
