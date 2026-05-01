import pandas as pd
from sqlalchemy import create_engine
import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor

DB_URI = 'postgresql+psycopg2://postgres:admin@127.0.0.1:5432/apartments'

def predict_energy_consumption():
    print("--- Starting Energy Consumption Prediction Workflow ---")
    engine = create_engine(DB_URI)
    
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
    df_train['solar_wm2'].fillna(0, inplace=True)
    
    X = df_train[['hour', 'day_of_week', 'is_weekend_int', 'solar_wm2']]
    y = df_train['target_power_w']

    X_train_split, X_test_split, y_train_split, y_test_split = train_test_split(X, y, test_size=0.2, random_state=42)

    models = {
        "Linear Regression": LinearRegression(),
        "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
        "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42)
    }

    print("\n--- Model Comparison Performance ---")
    best_model_name = ""
    best_model = None
    lowest_error = float('inf')

    for name, model in models.items():
        model.fit(X_train_split, y_train_split)
        predictions = model.predict(X_test_split)
        mae = mean_absolute_error(y_test_split, predictions)
        rmse = root_mean_squared_error(y_test_split, predictions)
        
        print(f"{name} -> MAE: {mae:.2f} W | RMSE: {rmse:.2f} W")
        
        if mae < lowest_error:
            lowest_error = mae
            best_model_name = name
            best_model = model

    print(f"\nWinner: {best_model_name} with an average error of {lowest_error:.2f} Watts")

    print(f"\n--- Training {best_model_name} on 100% of historical data ---")
    best_model.fit(X, y)

    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    tomorrow_date_key = int(tomorrow.strftime('%Y%m%d'))
    tomorrow_dow = tomorrow.isoweekday()
    tomorrow_is_weekend = 1 if tomorrow_dow >= 6 else 0
    
    forecast_query = f"""
        SELECT hour, AVG(value_wm2) as solar_wm2
        FROM gold.fact_solar_prediction
        WHERE date_key = {tomorrow_date_key}
        GROUP BY hour
    """
    df_forecast = pd.read_sql(forecast_query, engine)
    
    if df_forecast.empty:
        df_forecast = pd.DataFrame({'hour': range(24), 'solar_wm2': 0.0})

    df_future = pd.DataFrame({'hour': range(24)})
    df_future = df_future.merge(df_forecast, on='hour', how='left')
    df_future['solar_wm2'].fillna(0, inplace=True)
    df_future['day_of_week'] = tomorrow_dow
    df_future['is_weekend_int'] = tomorrow_is_weekend
    
    future_features = df_future[['hour', 'day_of_week', 'is_weekend_int', 'solar_wm2']]
    df_future['predicted_power_w'] = best_model.predict(future_features)
    
    df_future['date_key'] = tomorrow_date_key
    db_export = df_future[['date_key', 'hour', 'predicted_power_w']]
    
    try:
        db_export.to_sql('energy_forecast', engine, schema='predictions', if_exists='append', index=False)
        print(f"Successfully deployed! Saved {len(db_export)} energy predictions to database.")
    except Exception as e:
        print(f"Failed to save to database. Error: {e}")

if __name__ == "__main__":
    predict_energy_consumption()