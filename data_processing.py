import pandas as pd
import json
import os
import glob

# point this to the folder in the VM where data is
base_path = r"C:\RawData" 
output_path = os.path.join(base_path, "processed_data.csv")

def find_file(pattern):
    # search everywere in the VM folders
    search = os.path.join(base_path, "**", pattern)
    files = glob.glob(search, recursive=True)
    if not files:
        return None

    return max(files, key=os.path.getmtime)

def process_data():
    print("Starting the data process...")
    
    # finding files 
    sns_file = find_file("sensors_*.csv")
    rms_file = find_file("rooms_*.csv")
    json_file = find_file("*.json")

    if not sns_file or not rms_file or not json_file:
        print("Error: sum files are still missing in this machine!")
        return

    print(f"I found the files, lets start joining...")

    try:
        # Load the data into pandas DataFrames
        df_sns = pd.read_csv(sns_file)
        df_rms = pd.read_csv(rms_file)
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        df_read = pd.DataFrame(data)

        # replaces the JOINER node
        # join sensors with the rooms names
        df_mid = pd.merge(df_sns, df_rms, left_on='room_id', right_on='id', suffixes=('_s', '_r'))
        
        # join the result with the sensor values
        df_final = pd.merge(df_read, df_mid, left_on='sensor_id', right_on='id_s')

        # replace the COLUMN FILTER node
        cols = ['timestamp', 'value', 'name_r', 'sensor_type']
        df_final = df_final[cols]

        # Save to CSV
        df_final.to_csv(output_path, index=False)
        print("Done! the file is ready for analysis.")
        print(df_final.head(5)) 
        
    except Exception as e:
        print(f"Something went wrong during the join: {e}")

if __name__ == "__main__":
    process_data()
