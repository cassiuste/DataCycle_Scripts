import subprocess
import sys

python = r"C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe"

print("Starting Bronze ingestion...")
result = subprocess.run([python, r"C:\Scripts\run_bronze.py"])
if result.returncode != 0:
    print("Bronze FAILED, stopping pipeline.")
    sys.exit(1)

print("Starting Silver ETL...")
result = subprocess.run([python, r"C:\Scripts\silver_etl.py"])
if result.returncode != 0:
    print("Silver FAILED, stopping pipeline.")
    sys.exit(1)

print("Starting Gold ETL...")
subprocess.run([python, r"C:\Scripts\gold_etl.py"])
print("Pipeline complete.")