import csv
import os
from database.db import get_connection

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "raw_events.csv")


def load_raw_events() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS raw_events")
    cur.execute(
        """
        CREATE TABLE raw_events (
            event_id   TEXT,
            user_id    TEXT,
            event_type TEXT,
            created_at TEXT,
            device     TEXT,
            city       TEXT,
            source     TEXT
        )
        """
    )

    with open(RAW_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                r["event_id"], r["user_id"], r["event_type"],
                r["created_at"], r["device"], r["city"], r["source"],
            )
            for r in reader
        ]

    cur.executemany(
        "INSERT INTO raw_events VALUES (?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    conn.close()
    print(f"  ✅ Loaded {len(rows):,} raw events into SQLite")