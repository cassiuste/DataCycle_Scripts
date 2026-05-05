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

    query = """
        SELECT
            e.date_key,
            e.building_key,
            d.hour,
            d.day_of_week,
            CASE WHEN d.is_weekend THEN 1 ELSE 0 END AS is_weekend_int,
            COALESCE(AVG(s.value_wm2), 0)            AS solar_wm2,
            AVG(e.house_total_power_w)                AS actual_power_w
        FROM gold.fact_energy_consumption e
        JOIN gold.dim_date d
            ON e.date_key = d.date_key
            AND e.hour    = d.hour
            AND e.minute  = d.minute
        LEFT JOIN gold.fact_solar_prediction s
            ON d.date_key = s.date_key
            AND d.hour    = s.hour
        GROUP BY
            e.date_key, e.building_key, d.hour, d.day_of_week, d.is_weekend
        HAVING AVG(e.house_total_power_w) IS NOT NULL
        ORDER BY e.date_key, d.hour
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found. Exiting.")
        return

    print(f"Loaded {len(df)} rows of historical data.")

    df = df.sort_values(['date_key', 'hour']).reset_index(drop=True)
    split_idx = int(len(df) * 0.8)
    df_train = df.iloc[:split_idx]
    df_test  = df.iloc[split_idx:]

    features = ['hour', 'day_of_week', 'is_weekend_int', 'solar_wm2']
    X_train, y_train = df_train[features], df_train['actual_power_w']
    X_test,  y_test  = df_test[features],  df_test['actual_power_w']

    models = {
        "Linear Regression":   LinearRegression(),
        "Random Forest":       RandomForestRegressor(n_estimators=100, random_state=42),
        "Gradient Boosting":   GradientBoostingRegressor(n_estimators=100, random_state=42),
    }

    print("\n--- Model Comparison Performance ---")
    performance_rows = []
    best_model_name, best_model, lowest_mae = "", None, float('inf')

    for name, model in models.items():
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        mae  = mean_absolute_error(y_test, preds)
        rmse = root_mean_squared_error(y_test, preds)
        print(f"{name} -> MAE: {mae:.2f} W | RMSE: {rmse:.2f} W")

        performance_rows.append({
            "run_date":         datetime.date.today(),
            "pipeline":         "energy",
            "model_name":       name,
            "mae_w":            round(mae, 4),
            "rmse_w":           round(rmse, 4),
            "accuracy":         None,
            "f1_score":         None,
            "is_winner":        False,
            "trained_on_n_rows": len(df_train),
        })

        if mae < lowest_mae:
            lowest_mae     = mae
            best_model_name = name
            best_model      = model

    for row in performance_rows:
        if row["model_name"] == best_model_name:
            row["is_winner"] = True

    print(f"\nWinner: {best_model_name} — MAE: {lowest_mae:.2f} W")

    print(f"\n--- Re-training {best_model_name} on 100% of data ---")
    best_model.fit(df[features], df['actual_power_w'])

    df['predicted_power_w'] = best_model.predict(df[features])
    df['error_w']           = df['predicted_power_w'] - df['actual_power_w']
    df['model_name']        = best_model_name

    export_energy = df[[
        'date_key', 'hour', 'building_key',
        'predicted_power_w', 'actual_power_w', 'error_w', 'model_name'
    ]].copy()

    try:
        pd.DataFrame(performance_rows).to_sql(
            'model_performance', engine,
            schema='predictions', if_exists='append', index=False
        )
        print(f"Saved {len(performance_rows)} model metrics.")

        export_energy.to_sql(
            'energy_forecast', engine,
            schema='predictions', if_exists='replace', index=False
        )
        print(f"Saved {len(export_energy)} energy forecast rows.")
    except Exception as e:
        print(f"DB error: {e}")

if __name__ == "__main__":
    predict_energy_consumption()