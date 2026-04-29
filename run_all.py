# simple script to run everything in order
import subprocess

scripts = [
    "json_ingestion.py",
    "mysql_ingestion.py",
    "sftp_ingestion.py",
    "data_processing.py"
]

for script in scripts:
    print(f"Running {script}...")
    subprocess.run(["python", script])

print("All tasks finished!")