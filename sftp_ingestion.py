import paramiko
import os

sftp_host = "10.130.25.152"
sftp_port = 22
sftp_user = "Student"
sftp_pass = "3uw.AQ!SWxsDBm2zi3"

base_local_path = r"C:\RawData"

remote_dir = "/Meteo2"
local_folder = "meteo2"

print("Starting SFTP Ingestion")

try:
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_pass)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print(f"Successfully connected to SFTP server")

    total_downloaded = 0

    print(f"Checking remote directory: {remote_dir}...")
    
    try:
        # Change working directory on the SFTP server
        sftp.chdir(remote_dir)
        files = sftp.listdir()

        for file_name in files:
            if file_name.startswith("Pred_") and file_name.endswith(".csv"):
                
                # Extract date from filename
                date_part = file_name.replace("Pred_", "").replace(".csv", "")
                year, month, day = date_part.split("-")

                # Build local destination path
                final_dest_path = os.path.join(base_local_path, local_folder, year, month, day)
                os.makedirs(final_dest_path, exist_ok=True)

                local_file_path = os.path.join(final_dest_path, file_name)

                if not os.path.exists(local_file_path):
                    print(f"Downloading new file: {local_folder}/{year}/{month}/{day}/{file_name}")
                    sftp.get(file_name, local_file_path)
                    total_downloaded += 1

    except IOError:
        print(f"Warning: Could not find or access remote folder '{remote_dir}'.")

    sftp.close()
    transport.close()

    print("Status: Success")
    print(f"New files downloaded: {total_downloaded}")

except Exception as e:
    print(f"Error during SFTP connection or transfer: {e}")