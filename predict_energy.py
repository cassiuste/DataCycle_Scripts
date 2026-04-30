import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sqlalchemy import create_engine
import datetime

# Database connection
DB_URI = 'postgresql+psycopg2://postgres:admin@127.0.0.1:5432/apartments'

def predict_energy_consumption():
    print("--- Starting Energy Consumption Prediction ---")
    engine = create_engine(DB_URI)
    
    # Fetch Historical Training Data from GOLD
    train_query = """
        SELECT 
            d.hour,
            d.day_of_week,
            CASE WHEN d.is_weekend THEN 1 ELSE 0 END AS is_weekend_int,
            AVG(s.value_wm2) AS solar_wm2,
            AVG(e.house_total_power_w) AS target_power_w
        FROM gold.fact_energy_consumption e
        JOIN gold.dim_date d 
            ON e.date_key = d.date_key AND e.hour = d.hour AND e.minute = d.minute
        LEFT JOIN gold.fact_solar_prediction s 
            ON d.date_key = s.date_key AND d.hour = s.hour
        GROUP BY d.date_key, d.hour, d.day_of_week, d.is_weekend
        HAVING AVG(e.house_total_power_w) IS NOT NULL
    """
    df_train = pd.read_sql(train_query, engine)
    
    df_train['solar_wm2'].fillna(0, inplace=True) # Fill missing night-time solar data with 0
    
    X_train = df_train[['hour', 'day_of_week', 'is_weekend_int', 'solar_wm2']]
    y_train = df_train['target_power_w']

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    print(f"Model trained on {len(df_train)} historical hourly records.")

    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    tomorrow_date_key = int(tomorrow.strftime('%Y%m%d'))
    tomorrow_dow = tomorrow.isoweekday()
    tomorrow_is_weekend = 1 if tomorrow_dow >= 6 else 0
    
    forecast_query = f"""
        SELECT 
            hour,
            AVG(value_wm2) as solar_wm2
        FROM gold.fact_solar_prediction
        WHERE date_key = {tomorrow_date_key}
        GROUP BY hour
    """
    df_forecast = pd.read_sql(forecast_query, engine)
    
    # Fallback: If tomorrow's weather forecast hasn't been ingested yet
    if df_forecast.empty:
        print("Warning: No solar prediction found for tomorrow. Defaulting solar_wm2 to 0.")
        df_forecast = pd.DataFrame({'hour': range(24), 'solar_wm2': 0.0})

    # Ensure all 24 hours exist
    df_future = pd.DataFrame({'hour': range(24)})
    df_future = df_future.merge(df_forecast, on='hour', how='left')
    df_future['solar_wm2'].fillna(0, inplace=True)
    
    df_future['day_of_week'] = tomorrow_dow
    df_future['is_weekend_int'] = tomorrow_is_weekend
    
    #Predict Future Energy
    future_features = df_future[['hour', 'day_of_week', 'is_weekend_int', 'solar_wm2']]
    df_future['predicted_power_w'] = model.predict(future_features)
    
    #Save Predictions to the PREDICTIONS schema
    df_future['date_key'] = tomorrow_date_key
    db_export = df_future[['date_key', 'hour', 'predicted_power_w']]
    
    try:
        db_export.to_sql('energy_forecast', engine, schema='predictions', if_exists='append', index=False)
        print(f"Successfully saved {len(db_export)} energy predictions to predictions.energy_forecast!")
    except Exception as e:
        print(f"Failed to save to database. Error: {e}")

if __name__ == "__main__":
    predict_energy_consumption()