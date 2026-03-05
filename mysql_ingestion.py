import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine

db_config = {
    'host': '10.130.25.152',
    'port': 3306,
    'user': 'student',
    'password': 'widSN3Ey35fWVOxY',
    'database': 'pidb',
    'ssl_ca': r'C:\Scripts\MySQL_Keys\MySQL_Keys\ca-cert.pem',
    'ssl_cert': r'C:\Scripts\MySQL_Keys\MySQL_Keys\client-cert.pem',
    'ssl_key': r'C:\Scripts\MySQL_Keys\MySQL_Keys\client-key.pem'
}

base_dest_path = r"C:\RawData"
source_system = "apartments"

tables = [
    "Buildings", 
    "BuildingType", 
    "Devices", 
    "DIErrors", 
    "Rooms", 
    "Sensors"
]

print("Starting MySQL Extraction...")

try:
    print("Connecting to MySQL Database...")
    conn = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    print("Connection successful!")
    ssl_args = {
        'ssl_ca': db_config['ssl_ca'],
        'ssl_cert': db_config['ssl_cert'],
        'ssl_key': db_config['ssl_key']
    }
    
    engine = create_engine(conn, connect_args=ssl_args)
    print("Connection successful!")

    now = datetime.now()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    
    final_dest_path = os.path.join(base_dest_path, source_system, year, month, day)
    os.makedirs(final_dest_path, exist_ok=True)

    extracted_count = 0

    for table in tables:
        print(f"Extracting data from table: {table}...")
        
        query = f"SELECT * FROM {table}"
        
        df = pd.read_sql(query, engine)
        
        table_name = table.lower()  
        file_name = f"{table_name}_{year}_{month}_{day}.csv"
        file_path = os.path.join(final_dest_path, file_name)
        
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"Saved successfully: {file_name} ({len(df)} rows)")
        
        extracted_count += 1

    engine.dispose()

    print("Status: Success")
    print(f"Tables successfully extracted: {extracted_count} out of {len(tables)}")

except Exception as e:
    print(f"An error occurred: {e}")