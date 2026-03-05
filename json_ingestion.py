import os
import shutil
import subprocess

network_ip = r"\\10.130.25.152"
network_user = "Student"
network_pass = "3uw.AQ!SWxsDBm2zi3"
base_destination_path = r"C:\RawData"

source_path = r"\\10.130.25.152\Apartments"

print("Starting JSON ingestion process...")


auth_command = f'net use "{network_ip}" /user:{network_user} {network_pass}'
subprocess.run(auth_command, shell=True, capture_output=True)

try:
    # Get all files in the network directory
    files = os.listdir(source_path)

    for file_name in files:
        if file_name.endswith(".json"):
            
            # Extract date from file name (e.g., "01.06.2023 0000_JimmyLoup_received.json")
            parts = file_name.split(" ") 
            date_str = parts[0]
            
            day, month, year = date_str.split(".")
            
            # Identify if it belongs to Jimmy or Jeremie for the subfolder naming
            if "JimmyLoup" in file_name:
                subfolder = "jimmy_loup"
            elif "JeremieVianin" in file_name:
                subfolder = "jeremie_vianin"
            else:
                continue # Skips any file that doesn't belong to Jimmy or Jeremie

            # Build final destination path (Separates them into different folders)
            final_destination_path = os.path.join(base_destination_path, subfolder, year, month, day)

            # Create YYYY, MM, DD folders if they don't exist
            os.makedirs(final_destination_path, exist_ok=True)

            # Define full paths for source and destination
            source_file_path = os.path.join(source_path, file_name)
            destination_file_path = os.path.join(final_destination_path, file_name)
            
            # Copy only if it doesn't exist yet
            if not os.path.exists(destination_file_path):
                shutil.copy2(source_file_path, destination_file_path)
                print(f"New file ingested: {subfolder}/{year}/{month}/{day}/{file_name}")

except FileNotFoundError:
    print(f"Error: Could not access network path {source_path}.")