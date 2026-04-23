import subprocess
import sys

python = r"C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe"

print("Starting JSON ingestion...")
result1 = subprocess.run([python, r"C:\Scripts\json_ingestion.py"])
if result1.returncode == 0:
    print("JSON ingestion OK.")
else:
    print("JSON ingestion FAILED.")

print("Starting SFTP ingestion...")
result2 = subprocess.run([python, r"C:\Scripts\sftp_ingestion.py"])
if result2.returncode == 0:
    print("SFTP ingestion OK.")
else:
    print("SFTP ingestion FAILED.")