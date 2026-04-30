import subprocess
import os
from datetime import datetime
from pathlib import Path

BACKUP_DIR = Path("C:/Backups/postgres")
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

PG_DUMP = r"C:\Program Files\PostgreSQL\16\bin\pg_dump.exe"

DB_NAME  = "apartments"
DB_USER  = "postgres"
DB_HOST  = "localhost"
DB_PORT  = "5432"

# Keep only last 7 days of backups
KEEP_DAYS = 7

def backup():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = BACKUP_DIR / f"apartments_{timestamp}.backup"

    env = os.environ.copy()
    env["PGPASSWORD"] = "admin"

    cmd = [
        PG_DUMP,
        "-h", DB_HOST,
        "-p", DB_PORT,
        "-U", DB_USER,
        "-F", "c",          # custom format (compressed)
        "-b",               # include blobs
        "-v",               # verbose
        "-f", str(output_file),
        DB_NAME
    ]

    print(f"Starting backup → {output_file}")
    result = subprocess.run(cmd, env=env)

    if result.returncode == 0:
        size = output_file.stat().st_size / (1024*1024)
        print(f"Backup completed: {output_file} ({size:.1f} MB)")
    else:
        print(f"Backup FAILED!")

def cleanup_old_backups():
    """Delete backups older than KEEP_DAYS days."""
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(days=KEEP_DAYS)
    deleted = 0
    for f in BACKUP_DIR.glob("*.backup"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            deleted += 1
    if deleted:
        print(f"Deleted {deleted} old backup(s).")

if __name__ == "__main__":
    backup()
    cleanup_old_backups()