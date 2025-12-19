import sqlite3
from pathlib import Path
import logging


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "data.db" #path to db

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn

def create_events_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        facilityId INTEGER PRIMARY KEY,
        name TEXT,
        spots INTEGER,
        total INTEGER,
        available INTEGER,
        status TEXT,
        lat REAL,
        lon REAL,
        timestamp TEXT
    )
    """)
    conn.commit()
    conn.close()
    logging.info("Tables created or already exist.")


def insert_events(records): #upsert
    conn = get_connection()
    cursor = conn.cursor()
    print(f"Inserting records")
    for r in records:
        cursor.execute("""
        INSERT INTO events (facilityId, name, spots, total, available, status, lat, lon, timestamp)
        VALUES (:facility_id, :name, :spots, :total, :available, :status, :lat, :lon, :timestamp)
        ON CONFLICT(facilityId) DO UPDATE SET
            spots=excluded.spots,
            total=excluded.total,
            available=excluded.available,
            status=excluded.status,
            lat=excluded.lat,
            lon=excluded.lon,
            timestamp=excluded.timestamp
        """, r)
    conn.commit()
    conn.close()
    logging.info(f"Inserted/updated {len(records)} records into events.")
    print(f"Inserted/updated {len(records)} records into events.")

def create_daily_summary_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_summary (
        date TEXT PRIMARY KEY,
        total_facilities INTEGER,
        full_count INTEGER,
        almost_full_count INTEGER,
        available_count INTEGER,
        min_available INTEGER,
        max_available INTEGER,
        avg_available REAL,
        avg_free_spots REAL,
        available_pct REAL
    )
    """)
    print(f"Created daily_summary table if not exists")
    conn.commit()
    conn.close()


def insert_daily_summary(summary): #upsert
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO daily_summary (
        date,
        total_facilities,
        full_count,
        almost_full_count,
        available_count,
        min_available,
        max_available,
        avg_available,
        avg_free_spots,
        available_pct
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date) DO UPDATE SET
        total_facilities=excluded.total_facilities,
        full_count=excluded.full_count,
        almost_full_count=excluded.almost_full_count,
        available_count=excluded.available_count,
        min_available=excluded.min_available,
        max_available=excluded.max_available,
        avg_available=excluded.avg_available,
        avg_free_spots=excluded.avg_free_spots,
        available_pct=excluded.available_pct
    """, (
        summary['date'],
        summary['total_facilities'],
        summary['full_count'],
        summary['almost_full_count'],
        summary['available_count'],
        summary['min_available'],
        summary['max_available'],
        summary['avg_available'],
        summary['avg_free_spots'],
        summary['available_pct']
    ))

    conn.commit()
    conn.close()
    logging.info(f"Inserted/updated daily summary for {summary['date']}.")

    
def fetch_all_events():  #fetch some records for testing
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT spots, total, available, status
        FROM events
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

    
