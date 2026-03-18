-- =========================
-- SCHEMA
-- =========================
CREATE SCHEMA IF NOT EXISTS silver;

-- =========================
-- STATIC (CSV)
-- =========================
CREATE TABLE IF NOT EXISTS silver.buildings (
    id_building INT PRIMARY KEY,
    id_user INT,
    id_building_type INT,
    house_name TEXT,
    first_name TEXT,
    last_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    address TEXT,
    npa TEXT,
    city TEXT,
    building_year INT,
    phone_number TEXT,
    nb_people INT,
    is_heating_on BOOLEAN
);

-- =========================
-- EVENTS
-- =========================
CREATE TABLE IF NOT EXISTS silver.events (
    id SERIAL PRIMARY KEY,
    user_name TEXT,
    datetime TIMESTAMP,
    api_token TEXT
);

-- =========================
-- PLUGS
-- =========================
CREATE TABLE IF NOT EXISTS silver.plugs (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES silver.events(id),
    room TEXT,
    power FLOAT,
    total INT,
    temperature FLOAT,
    switch_state BOOLEAN
);

-- =========================
-- DOORS / WINDOWS
-- =========================
CREATE TABLE IF NOT EXISTS silver.doors_windows (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES silver.events(id),
    room TEXT,
    type TEXT,
    state TEXT,
    battery INT
);

-- =========================
-- MOTIONS
-- =========================
CREATE TABLE IF NOT EXISTS silver.motions (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES silver.events(id),
    room TEXT,
    motion BOOLEAN,
    light INT,
    temperature FLOAT
);

-- =========================
-- METEOS
-- =========================
CREATE TABLE IF NOT EXISTS silver.meteos (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES silver.events(id),
    room TEXT,
    temperature FLOAT,
    co2 INT,
    humidity INT,
    noise INT,
    pressure FLOAT
);

-- =========================
-- HUMIDITIES
-- =========================
CREATE TABLE IF NOT EXISTS silver.humidities (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES silver.events(id),
    room TEXT,
    temperature FLOAT,
    humidity FLOAT,
    device_power INT
);

-- =========================
-- CONSUMPTIONS
-- =========================
CREATE TABLE IF NOT EXISTS silver.consumptions (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES silver.events(id),
    power1 FLOAT,
    power2 FLOAT,
    power3 FLOAT,
    total_power FLOAT,
    voltage1 FLOAT,
    voltage2 FLOAT,
    voltage3 FLOAT
);