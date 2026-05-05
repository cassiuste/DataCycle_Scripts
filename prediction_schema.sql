CREATE SCHEMA IF NOT EXISTS predictions;

CREATE TABLE IF NOT EXISTS predictions.model_performance (
    run_key        SERIAL PRIMARY KEY,
    run_date       DATE NOT NULL,
    pipeline       VARCHAR(50) NOT NULL, 
    model_name     VARCHAR(100) NOT NULL,
    mae_w          FLOAT,        
    rmse_w         FLOAT,        
    accuracy       FLOAT,        
    f1_score       FLOAT,        
    is_winner      BOOLEAN NOT NULL,
    trained_on_n_rows INT,
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions.energy_forecast (
    forecast_key      SERIAL PRIMARY KEY,
    date_key          INT NOT NULL,
    hour              INT NOT NULL,
    building_key      INT NOT NULL,
    predicted_power_w FLOAT NOT NULL,
    actual_power_w    FLOAT,         
    error_w           FLOAT,         
    model_name        VARCHAR(100),
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions.presence_forecast (
    forecast_key          SERIAL PRIMARY KEY,
    date_key              INT NOT NULL,
    hour                  INT NOT NULL,
    building_key          INT NOT NULL,
    room_key              INT NOT NULL,
    room_name             VARCHAR(100),
    is_present_predicted  INT NOT NULL,   
    is_present_actual     INT,            
    is_correct            BOOLEAN,        
    model_name            VARCHAR(100),
    created_at            TIMESTAMP DEFAULT NOW()
);