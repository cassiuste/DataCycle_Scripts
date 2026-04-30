import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import create_engine
import datetime

DB_URI = 'postgresql+psycopg2://postgres:admin@127.0.0.1:5432/apartments'

def predict_room_presence():
    print("--- Starting Room Presence Prediction ---")
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

    # Define our Features (X) and what we want to predict (y)
    X = df[['room_code', 'hour', 'day_of_week', 'is_weekend_int']]
    y = df['target_presence']

    # Training the Model
    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    model.fit(X, y)
    print(f"Model trained on {len(df)} historical records.")

    future_data = []
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    tomorrow_date_key = int(tomorrow.strftime('%Y%m%d'))
    tomorrow_dow = tomorrow.isoweekday()
    tomorrow_is_weekend = 1 if tomorrow_dow >= 6 else 0

    # Create a row for every single room, for every single hour of tomorrow
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
    future_df['is_present_predicted'] = model.predict(future_features)
    
    # We only keep the columns that match our SQL table structure
    db_export = future_df[['date_key', 'hour', 'room_name', 'is_present_predicted']]
    
    try:
        db_export.to_sql('presence_forecast', engine, schema='predictions', if_exists='append', index=False)
        print(f"Successfully saved {len(db_export)} presence predictions to predictions.presence_forecast")
    except Exception as e:
        print(f"Failed to save to database. Error: {e}")

if __name__ == "__main__":
    predict_room_presence()