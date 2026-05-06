@echo off
echo Starting deployment...

mkdir C:\RawData\static
mkdir C:\RawData\meteo2
mkdir C:\Logs\bronze
mkdir C:\Logs\silver
mkdir C:\Logs\gold
mkdir C:\Logs\sac
mkdir C:\SAC_Export
mkdir C:\Backups\postgres
mkdir C:\Scripts

copy ".\scripts\*.py" "C:\Scripts\"
copy ".\config.json" "C:\Scripts\"
copy ".\static.csv" "C:\RawData\static\data_static.csv"

pip install -r requirements.txt

set PGPASSWORD=admin
psql -U postgres -h localhost -d apartments -f "setup_database.sql"

:: 1. Bronze (JSON) - Every 5 minutes
schtasks /create /tn "SH_Ingest_JSON" /tr "python C:\Scripts\json_ingestion.py" /sc minute /mo 5 /f

:: 2. Silver ETL - Every 15 minutes
schtasks /create /tn "SH_ETL_Silver" /tr "python C:\Scripts\silver_etl.py" /sc minute /mo 15 /f

:: 3. Gold ETL + SAC Export - Every 30 minutes
schtasks /create /tn "SH_ETL_Gold_SAC" /tr "python C:\Scripts\gold_etl.py" /sc minute /mo 30 /f

:: 4. Bronze (SFTP Meteo) - Every 3 hours
schtasks /create /tn "SH_Ingest_SFTP" /tr "python C:\Scripts\sftp_ingestion.py" /sc hourly /mo 3 /f

:: 5. Database Backup - Daily at 00:30
schtasks /create /tn "SH_DB_Backup" /tr "python C:\Scripts\backup.py" /sc daily /st 00:30 /f

:: 6. Machine Learning Predictions - Daily at 01:00
schtasks /create /tn "SH_ML_Predictions" /tr "python C:\Scripts\run_ml.py" /sc daily /st 01:00 /f

echo Deployment complete.
pause