-- =============================================================================
-- SILVER LAYER SCHEMA
-- Sources: Smart Home IoT (JSON), Solar Prediction (CSV), Static Building Data (CSV)
-- =============================================================================


-- =============================================================================
-- 1. STATIC / REFERENCE TABLES
-- =============================================================================

-- Users and building metadata (from data_static.csv)
CREATE TABLE silver.dim_buildings (
    id_building         INT             NOT NULL,
    id_user             INT             NOT NULL,
    id_building_type    INT             NOT NULL,
    house_name          VARCHAR(100)    NOT NULL,
    first_name          VARCHAR(100)    NOT NULL,
    last_name           VARCHAR(100)    NOT NULL,
    latitude            DOUBLE PRECISION          NOT NULL,
    longitude           DOUBLE PRECISION          NOT NULL,
    address             VARCHAR(255)    NOT NULL,
    npa                 INT             NOT NULL,
    city                VARCHAR(100)    NOT NULL,
    building_year       INT,
    phone_number        VARCHAR(20),
    nb_people           INT,
    is_heating_on       BOOLEAN,

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id_building)
);


-- =============================================================================
-- 2. SMART HOME IoT TABLES
--    Source: RawData/{username}/{year}/{month}/{day}/*.json
--    One JSON file = one snapshot at a given datetime for one user/building
-- =============================================================================

-- Master event table: one row per JSON file received
-- Links all sub-tables together via event_id
CREATE TABLE silver.iot_events (
    event_id            BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_building         INT             NOT NULL,   -- FK to dim_buildings
    username            VARCHAR(100)    NOT NULL,   -- e.g. "JeremieVianin"
    event_datetime      TIMESTAMP       NOT NULL,   -- parsed from "datetime" field in JSON
    source_file         VARCHAR(512),               -- original file path for traceability

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- Smart plugs (one row per plug per event)
-- Source: JSON .plugs.{room}
CREATE TABLE silver.iot_plugs (
    plug_id             BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            BIGINT          NOT NULL,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,   -- e.g. "Office", "Livingroom"
    time_plug           BIGINT,                     -- Unix timestamp from device
    power_w             DOUBLE PRECISION,                     -- Instantaneous power (Watts)
    overpower_w         DOUBLE PRECISION,
    is_valid            BOOLEAN,
    counter1_a          DOUBLE PRECISION,                     -- Current phase 1 (Amps)
    counter2_a          DOUBLE PRECISION,
    counter3_a          DOUBLE PRECISION,
    total_wh            BIGINT,                     -- Cumulative energy counter (Wh)
    temperature_c       DOUBLE PRECISION,                     -- Plug internal temperature (°C)
    overtemperature     BOOLEAN,
    switch_on           BOOLEAN,                    -- Plug relay state

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id)   REFERENCES silver.iot_events(event_id),
    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- Doors and windows sensors (one row per sensor per event)
-- Source: JSON .doorsWindows.{room}[{index}]
CREATE TABLE silver.iot_doors_windows (
    door_window_id      BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            BIGINT          NOT NULL,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,   -- e.g. "Kitchen", "Office", "Bdroom"
    sensor_index        INT             NOT NULL,   -- Position in the room array (0-based)
    sensor_type         VARCHAR(20),                -- "Door" or "Window"
    is_open             BOOLEAN,                    -- true = "on" (open), false = "off" (closed)
    battery_pct         INT,
    defense_level       INT,

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id)    REFERENCES silver.iot_events(event_id),
    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- Motion sensors (one row per sensor location per event)
-- Source: JSON .motions.{room}
CREATE TABLE silver.iot_motions (
    motion_id           BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            BIGINT          NOT NULL,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,   -- e.g. "Kitchen", "Office", "Livingroom"
    motion_detected     BOOLEAN,
    light_lux           INT,                        -- Ambient light level (lux)
    temperature_c       DOUBLE PRECISION,

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id)    REFERENCES silver.iot_events(event_id),
    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- Indoor meteo sensors (one row per station per event)
-- Source: JSON .meteos.meteo.{station}
-- Stations can be: Livingroom, Office, Bdroom, Outdoor
CREATE TABLE silver.iot_meteo_indoor (
    meteo_indoor_id     BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            BIGINT          NOT NULL,
    id_building         INT             NOT NULL,
    station             VARCHAR(100)    NOT NULL,   -- e.g. "Livingroom", "Office", "Bdroom", "Outdoor"
    temperature_c       DOUBLE PRECISION,
    humidity_pct        DOUBLE PRECISION,
    co2_ppm             INT,                        -- NULL for Outdoor station
    noise_db            INT,                        -- NULL for non-Livingroom stations
    pressure_hpa        DOUBLE PRECISION,                     -- NULL for non-Livingroom stations
    absolute_pressure_hpa DOUBLE PRECISION,                  -- NULL for non-Livingroom stations
    battery_pct         INT,                        -- NULL for Livingroom (mains powered)

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id)    REFERENCES silver.iot_events(event_id),
    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- Humidity sensors (one row per sensor location per event)
-- Source: JSON .humidities.{room}
-- Typically found in Laundry, Bhroom (bathroom)
CREATE TABLE silver.iot_humidities (
    humidity_id         BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            BIGINT          NOT NULL,
    id_building         INT             NOT NULL,
    room                VARCHAR(100)    NOT NULL,   -- e.g. "Laundry", "Bhroom"
    temperature_c       DOUBLE PRECISION,
    humidity_pct        DOUBLE PRECISION,
    device_power_pct    INT,                        -- Battery/power level of device

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id)    REFERENCES silver.iot_events(event_id),
    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- Whole-house energy consumption (one row per event)
-- Source: JSON .consumptions.House
-- 3-phase electrical meter data
CREATE TABLE silver.iot_consumption (
    consumption_id      BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            BIGINT          NOT NULL,
    id_building         INT             NOT NULL,
    time_consumption    BIGINT,                     -- Unix timestamp from meter

    -- Power per phase (Watts)
    power1_w            DOUBLE PRECISION,
    power2_w            DOUBLE PRECISION,
    power3_w            DOUBLE PRECISION,
    total_power_w       DOUBLE PRECISION,                     -- Sum of all 3 phases

    -- Power factor per phase (dimensionless, -1 to 1)
    pf1                 DOUBLE PRECISION,
    pf2                 DOUBLE PRECISION,
    pf3                 DOUBLE PRECISION,

    -- Current per phase (Amps)
    current1_a          DOUBLE PRECISION,
    current2_a          DOUBLE PRECISION,
    current3_a          DOUBLE PRECISION,

    -- Voltage per phase (Volts)
    voltage1_v          DOUBLE PRECISION,
    voltage2_v          DOUBLE PRECISION,
    voltage3_v          DOUBLE PRECISION,

    -- Validity flags
    is_valid1           BOOLEAN,
    is_valid2           BOOLEAN,
    is_valid3           BOOLEAN,

    switch_on           BOOLEAN,

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id)    REFERENCES silver.iot_events(event_id),
    FOREIGN KEY (id_building) REFERENCES silver.dim_buildings(id_building)
);


-- =============================================================================
-- 3. SOLAR IRRADIANCE PREDICTION TABLE
--    Source: RawData/meteo2/{year}/{month}/{day}/Pred_{date}.csv
--    Columns: Time, Value, Prediction, Site, Measurement, Unit
--    Note: Value = -99999.0 is a sentinel for missing/unavailable data
-- =============================================================================

CREATE TABLE silver.meteo_solar_prediction (
    prediction_id       BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    prediction_time     TIMESTAMP       NOT NULL,   -- Parsed from "Time" (UTC)
    value_wm2           DOUBLE PRECISION,                     -- Irradiance in W/m² (-99999 = missing)
    is_missing          BOOLEAN         GENERATED ALWAYS AS (value_wm2 = -99999.0) STORED,  -- convenience flag
    prediction_horizon  VARCHAR(10),                -- From "Prediction" column (e.g. "00")
    site                VARCHAR(200),               -- e.g. "Aadorf / Tänikon"
    measurement_type    VARCHAR(100),               -- e.g. "PRED_GLOB_ctrl"
    unit                VARCHAR(50),                -- e.g. "Watt/m2"
    source_date         DATE            NOT NULL,   -- Date of the prediction file (from filename)
    source_file         VARCHAR(512),               -- Original file path for traceability

    -- Audit
    ingested_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);


-- =============================================================================
-- 4. USEFUL INDEXES
-- =============================================================================

-- IoT event lookups by building and time
CREATE INDEX idx_iot_events_building_dt   ON silver.iot_events(id_building, event_datetime);
CREATE INDEX idx_iot_events_datetime      ON silver.iot_events(event_datetime);

-- Sub-table joins via event_id
CREATE INDEX idx_plugs_event              ON silver.iot_plugs(event_id);
CREATE INDEX idx_plugs_building_room      ON silver.iot_plugs(id_building, room);

CREATE INDEX idx_dw_event                 ON silver.iot_doors_windows(event_id);
CREATE INDEX idx_dw_building_room         ON silver.iot_doors_windows(id_building, room);

CREATE INDEX idx_motions_event            ON silver.iot_motions(event_id);
CREATE INDEX idx_motions_building_room    ON silver.iot_motions(id_building, room);

CREATE INDEX idx_meteo_indoor_event       ON silver.iot_meteo_indoor(event_id);
CREATE INDEX idx_meteo_indoor_station     ON silver.iot_meteo_indoor(id_building, station);

CREATE INDEX idx_humidities_event         ON silver.iot_humidities(event_id);
CREATE INDEX idx_humidities_room          ON silver.iot_humidities(id_building, room);

CREATE INDEX idx_consumption_event        ON silver.iot_consumption(event_id);
CREATE INDEX idx_consumption_building     ON silver.iot_consumption(id_building);

-- Solar prediction lookups
CREATE INDEX idx_solar_pred_time          ON silver.meteo_solar_prediction(prediction_time);
CREATE INDEX idx_solar_pred_source_date   ON silver.meteo_solar_prediction(source_date);
CREATE INDEX idx_solar_pred_site          ON silver.meteo_solar_prediction(site);
