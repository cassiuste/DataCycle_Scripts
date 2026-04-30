import subprocess
import sys

python = r"C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe"

print("Starting Energy Prediction...")
result = subprocess.run([python, r"C:\Scripts\predict_energy.py"])
if result.returncode != 0:
    print("Energy Prediction FAILED, stopping pipeline.")
    sys.exit(1)

print("Starting Room Presence Prediction...")
subprocess.run([python, r"C:\Scripts\predict_presence.py"])
print("Pipeline complete.")