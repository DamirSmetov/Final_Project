import sqlite3
from pathlib import Path
from datetime import date
import logging
import pandas as pd
from db_utils import create_daily_summary_table, fetch_all_events, insert_daily_summary

def run_daily_analytics():
    rows = fetch_all_events()#fetching some records for testing
    create_daily_summary_table()

    if not rows:
        logging.warning("No data in events table")
        return

    df = pd.DataFrame(rows, columns=["spots", "total", "available", "status"])

    total_facilities = len(df)

    # metrics
    available_values = df["available"]
    free_spots_values = df["spots"] - df["total"]

    full_count = (df["status"] == "Full").sum()
    almost_full_count = (df["status"] == "Almost Full").sum()
    available_count = (df["status"] == "Available").sum()

    min_available = available_values.min()
    max_available = available_values.max()
    avg_available = round(available_values.mean(), 2)

    avg_free_spots = round(free_spots_values.mean(), 2)
    available_pct = round((available_count / total_facilities) * 100, 2)

    today = date.today().isoformat()
    
    summary = {
        "date": today,
        "total_facilities": total_facilities,
        "full_count": int(full_count),
        "almost_full_count": int(almost_full_count),
        "available_count": int(available_count),
        "min_available": int(min_available),
        "max_available": int(max_available),
        "avg_available": avg_available,
        "avg_free_spots": avg_free_spots,
        "available_pct": available_pct
    }
    
    insert_daily_summary(summary)
    
    logging.info(f"Daily analytics computed for {today}")

if __name__ == "__main__":
    run_daily_analytics()
