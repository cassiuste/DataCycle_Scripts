-- SILVER LAYER
CREATE SCHEMA IF NOT EXISTS silver;
CREATE TABLE IF NOT EXISTS silver.dim_buildings (
    id_building INT PRIMARY KEY, id_user INT, id_building_type INT, house_name VARCHAR(100),
    first_name VARCHAR(100), last_name VARCHAR(100), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
    address VARCHAR(255), npa INT, city VARCHAR(100), building_year INT, phone_number VARCHAR(20),
    nb_people INT, is_heating_on BOOLEAN, ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.iot_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, id_building INT REFERENCES silver.dim_buildings(id_building),
    username VARCHAR(100), event_datetime TIMESTAMP, source_file VARCHAR(512), ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GOLD LAYER
CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.agg_consumption_hourly (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, id_building INT, hour_ts TIMESTAMP,
    avg_total_power_w DOUBLE PRECISION, estimated_energy_wh DOUBLE PRECISION, UNIQUE (id_building, hour_ts)
);

-- PREDICTIONS LAYER
CREATE SCHEMA IF NOT EXISTS predictions;
CREATE TABLE IF NOT EXISTS predictions.model_performance (
    run_key SERIAL PRIMARY KEY, run_date DATE, pipeline VARCHAR(50), model_name VARCHAR(100),
    mae_w FLOAT, rmse_w FLOAT, accuracy FLOAT, f1_score FLOAT, is_winner BOOLEAN, created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS predictions.energy_forecast (
    forecast_key SERIAL PRIMARY KEY, date_key INT, hour INT, building_key INT, 
    predicted_power_w FLOAT, actual_power_w FLOAT, error_w FLOAT, model_name VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS predictions.presence_forecast (
    forecast_key SERIAL PRIMARY KEY, date_key INT NOT NULL, hour INT NOT NULL, building_key INT NOT NULL,
    room_key INT NOT NULL, room_name VARCHAR(100), is_present_predicted INT NOT NULL, is_present_actual INT, 
    is_correct BOOLEAN, model_name VARCHAR(100), created_at TIMESTAMP DEFAULT NOW()
);