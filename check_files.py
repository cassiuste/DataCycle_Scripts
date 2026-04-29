import os
from datetime import datetime

#check current date
now = datetime.now()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

print(f"--- Diagnostic Start ---")
print(f"System Date: {year}-{month}-{day}")

# check base directory
base_path = r"C:\RawData"
if os.path.exists(base_path):
    print(f"OK: {base_path} exists.")
else:
    print(f"ERROR: {base_path} DOES NOT exist. Check your C: drive.")

#list folders inside RawData
try:
    print(f"Content of {base_path}: {os.listdir(base_path)}")
except Exception as e:
    print(f"Could not list base folder: {e}")

# check the specific expected file
target_file = os.path.join(base_path, "apartments", year, month, day, f"sensors_{year}_{month}_{day}.csv")
print(f"Looking for: {target_file}")

if os.path.exists(target_file):
    print("SUCCESS: File found!")
else:
    print("FAILED: File not found.")
    
    # test the paths
    test_path = os.path.join(base_path, "apartments")
    if os.path.exists(test_path):
        print(f"The path exists up to 'apartments'. Subfolders: {os.listdir(test_path)}")
    else:
        print("The path 'C:\\RawData\\apartments' does not even exist.")

print(f"--- Diagnostic End ---")