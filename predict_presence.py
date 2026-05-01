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
            r.room_name,
            d.hour,
            d.day_of_week,
            CASE WHEN d.is_weekend THEN 1 ELSE 0 END AS is_weekend_int,
            MAX(CASE WHEN f.motion_detected = true THEN 1 ELSE 0 END) AS target_presence
        FROM gold.fact_room_presence f
        JOIN gold.dim_date d 
            ON f.date_key = d.date_key AND f.hour = d.hour AND f.minute = d.minute
        JOIN gold.dim_room r 
            ON f.room_key = r.room_key
        GROUP BY 
            r.room_name,
            d.date_key,
            d.hour,
            d.day_of_week,
            d.is_weekend
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No historical data found. Exiting.")
        return
    
    df['room_code'] = df['room_name'].astype('category').cat.codes
    room_mapping = dict(enumerate(df['room_name'].astype('category').cat.categories))

    X = df[['room_code', 'hour', 'day_of_week', 'is_weekend_int']]
    y = df['target_presence']

    X_train_split, X_test_split, y_train_split, y_test_split = train_test_split(X, y, test_size=0.2, random_state=42)

    models = {
        "Logistic Regression": LogisticRegression(class_weight='balanced', max_iter=1000),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, random_state=42)
    }

    print("\n--- Model Comparison Performance ---")
    best_model_name = ""
    best_model = None
    highest_score = -1 

    for name, model in models.items():
        model.fit(X_train_split, y_train_split)
        predictions = model.predict(X_test_split)
        
        accuracy = accuracy_score(y_test_split, predictions)
        f1 = f1_score(y_test_split, predictions, zero_division=0)
        
        print(f"{name} -> Accuracy: {accuracy:.2f} | F1-Score: {f1:.2f}")
        
        if f1 > highest_score:
            highest_score = f1
            best_model_name = name
            best_model = model

    print(f"\nWinner: {best_model_name} with an F1-Score of {highest_score:.2f}")

    print(f"\n--- Training {best_model_name} on 100% of historical data ---")
    best_model.fit(X, y)

    future_data = []
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    tomorrow_date_key = int(tomorrow.strftime('%Y%m%d'))
    tomorrow_dow = tomorrow.isoweekday()
    tomorrow_is_weekend = 1 if tomorrow_dow >= 6 else 0

    for room_code, room_name in room_mapping.items():
        for hr in range(24):
            future_data.append({
                'date_key': tomorrow_date_key,
                'hour': hr,
                'room_name': room_name,
                'room_code': room_code,
                'day_of_week': tomorrow_dow,
                'is_weekend_int': tomorrow_is_weekend
            })
            
    future_df = pd.DataFrame(future_data)
    
    future_features = future_df[['room_code', 'hour', 'day_of_week', 'is_weekend_int']]
    future_df['is_present_predicted'] = best_model.predict(future_features)
    
    db_export = future_df[['date_key', 'hour', 'room_name', 'is_present_predicted']]
    
    try:
        db_export.to_sql('presence_forecast', engine, schema='predictions', if_exists='append', index=False)
        print(f"Successfully deployed! Saved {len(db_export)} presence predictions to database.")
    except Exception as e:
        print(f"Failed to save to database. Error: {e}")

if __name__ == "__main__":
    predict_room_presence()