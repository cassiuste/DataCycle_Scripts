import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

# load the data 
path = r"C:\RawData\processed_data.csv"

def simple_forecast():
    print("starting the machine learning part...")
    df = pd.read_csv(path)
    
    # convert time to a simple number
    df['time_index'] = np.arange(len(df))
    
    X = df[['time_index']] 
    y = df['value']       
    

    model = LinearRegression()
    model.fit(X, y)
    

    next_points = np.array([[len(df) + i] for i in range(10)])
    prediction = model.predict(next_points)
    
    print("prediction for the next hours:")
    print(prediction)

if __name__ == "__main__":
    simple_forecast()