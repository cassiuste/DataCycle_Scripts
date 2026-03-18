import os
import json
import psycopg2
import random
from datetime import datetime

BASE_DIR = "../RawData"

LIMIT_FILES_PER_FOLDER = 10  # 🔥 TEST

# =========================
# DB
# =========================
conn = psycopg2.connect(
    dbname="apartments",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()


# =========================
# NORMALIZATION HELPERS
# =========================

def safe_float(val):
    try:
        return float(val)
    except:
        return None

def safe_int(val):
    try:
        return int(val)
    except:
        return None


# =========================
# INSERT
# =========================

def insert_event(data):
    dt = datetime.strptime(data["datetime"], "%d.%m.%Y %H:%M")

    cur.execute("""
        INSERT INTO silver.events (user_name, datetime, api_token)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (data.get("user"), dt, data.get("api_token")))

    return cur.fetchone()[0]


def insert_plugs(event_id, plugs):
    for room, p in plugs.items():
        cur.execute("""
            INSERT INTO silver.plugs (event_id, room, power, total, temperature, switch_state)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            event_id,
            room,
            safe_float(p.get("power")),
            safe_int(p.get("total")),
            safe_float(p.get("temperature")),
            p.get("switch")
        ))


def insert_meteos(event_id, meteos):
    for room, m in meteos.get("meteo", {}).items():
        cur.execute("""
            INSERT INTO silver.meteos (event_id, room, temperature, co2, humidity, noise, pressure)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            event_id,
            room,
            safe_float(m.get("Temperature")),
            safe_int(m.get("CO2")),
            safe_int(m.get("Humidity")),
            safe_int(m.get("Noise")),
            safe_float(m.get("Pressure"))
        ))


def insert_consumption(event_id, cons):
    house = cons.get("House", {})

    cur.execute("""
        INSERT INTO silver.consumptions (event_id, power1, power2, power3, total_power)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        event_id,
        safe_float(house.get("power1")),
        safe_float(house.get("power2")),
        safe_float(house.get("power3")),
        safe_float(house.get("total_power"))
    ))


# =========================
# PROCESS FILE
# =========================

def process_json(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        event_id = insert_event(data)

        if "plugs" in data:
            insert_plugs(event_id, data["plugs"])

        if "meteos" in data:
            insert_meteos(event_id, data["meteos"])

        if "consumptions" in data:
            insert_consumption(event_id, data["consumptions"])

    except Exception as e:
        print(f"❌ {file_path} -> {e}")


# =========================
# TEST MODE (10 files / folder)
# =========================

def process_sample(base_dir):
    for root, dirs, files in os.walk(base_dir):
        json_files = [f for f in files if f.endswith(".json")]

        if not json_files:
            continue

        print(f"\n📂 {root} ({len(json_files)} fichiers)")

        sample_files = random.sample(
            json_files,
            min(LIMIT_FILES_PER_FOLDER, len(json_files))
        )

        for file in sample_files:
            process_json(os.path.join(root, file))


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    print("🚀 TEST ETL (10 fichiers par dossier)")
    process_sample(BASE_DIR)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ DONE")