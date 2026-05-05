import pandas as pd
from sqlalchemy import create_engine
import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

DB_URI = 'postgresql+psycopg2://postgres:admin@127.0.0.1:5432/apartments'

def predict_room_presence():
    print("--- Starting Room Presence Prediction Workflow ---")
    engine = create_engine(DB_URI)

    query = """
        SELECT
            f.date_key,
            f.building_key,
            f.room_key,
            r.room_name,
            d.hour,
            d.day_of_week,
            CASE WHEN d.is_weekend THEN 1 ELSE 0 END AS is_weekend_int,
            MAX(CASE WHEN f.motion_detected = true THEN 1 ELSE 0 END) AS actual_presence
        FROM gold.fact_room_presence f
        JOIN gold.dim_date d
            ON f.date_key = d.date_key
            AND f.hour    = d.hour
            AND f.minute  = d.minute
        JOIN gold.dim_room r
            ON f.room_key = r.room_key
        GROUP BY
            f.date_key, f.building_key, f.room_key, r.room_name,
            d.hour, d.day_of_week, d.is_weekend
        ORDER BY f.date_key, d.hour
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found. Exiting.")
        return

    print(f"Loaded {len(df)} rows of historical data.")

    df['room_code'] = df['room_name'].astype('category').cat.codes

    df = df.sort_values(['date_key', 'hour']).reset_index(drop=True)
    split_idx = int(len(df) * 0.8)
    df_train = df.iloc[:split_idx]
    df_test  = df.iloc[split_idx:]

    features = ['room_code', 'hour', 'day_of_week', 'is_weekend_int']
    X_train, y_train = df_train[features], df_train['actual_presence']
    X_test,  y_test  = df_test[features],  df_test['actual_presence']

    models = {
        "Logistic Regression": LogisticRegression(class_weight='balanced', max_iter=1000),
        "Random Forest":       RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'),
        "Gradient Boosting":   GradientBoostingClassifier(n_estimators=100, random_state=42),
    }

    print("\n--- Model Comparison Performance ---")
    performance_rows = []
    best_model_name, best_model, best_f1 = "", None, -1.0

    for name, model in models.items():
        model.fit(X_train, y_train)
        preds    = model.predict(X_test)
        accuracy = accuracy_score(y_test, preds)
        f1       = f1_score(y_test, preds, zero_division=0)
        print(f"{name} -> Accuracy: {accuracy:.2f} | F1: {f1:.2f}")

        performance_rows.append({
            "run_date":          datetime.date.today(),
            "pipeline":          "presence",
            "model_name":        name,
            "mae_w":             None,
            "rmse_w":            None,
            "accuracy":          round(accuracy, 4),
            "f1_score":          round(f1, 4),
            "is_winner":         False,
            "trained_on_n_rows": len(df_train),
        })

        if f1 > best_f1:
            best_f1         = f1
            best_model_name = name
            best_model      = model

    for row in performance_rows:
        if row["model_name"] == best_model_name:
            row["is_winner"] = True

    print(f"\nWinner: {best_model_name} — F1: {best_f1:.2f}")

    print(f"\n--- Re-training {best_model_name} on 100% of data ---")
    best_model.fit(df[features], df['actual_presence'])

    df['is_present_predicted'] = best_model.predict(df[features])
    df['is_correct']           = df['is_present_predicted'] == df['actual_presence']
    df['model_name']           = best_model_name

    export_presence = df[[
        'date_key', 'hour', 'building_key', 'room_key', 'room_name',
        'is_present_predicted', 'actual_presence', 'is_correct', 'model_name'
    ]].rename(columns={'actual_presence': 'is_present_actual'})


    try:
        pd.DataFrame(performance_rows).to_sql(
            'model_performance', engine,
            schema='predictions', if_exists='append', index=False
        )
        print(f"Saved {len(performance_rows)} model metrics.")

        export_presence.to_sql(
            'presence_forecast', engine,
            schema='predictions', if_exists='replace', index=False
        )
        print(f"Saved {len(export_presence)} presence forecast rows.")
    except Exception as e:
        print(f"DB error: {e}")

if __name__ == "__main__":
    predict_room_presence()