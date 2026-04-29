import subprocess

# list of scripts to run in order
scripts = [
    "json_ingestion.py",
    "mysql_ingestion.py",
    "sftp_ingestion.py",
    "data_processing.py",
    "to_influx.py"
]

def run_project():
    print("--- STARTING FULL DATA CYCLE ---")
    for script in scripts:
        print(f"Executing {script}...")
        result = subprocess.run(["python", script], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"{script} finished ok.")
        else:
            print(f"Error in {script}: {result.stderr}")
            break
    print("--- PROCESS FINISHED ---")

if __name__ == "__main__":
    run_project()