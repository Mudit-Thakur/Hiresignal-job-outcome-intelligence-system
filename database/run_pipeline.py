import os
import sys
import csv
import time

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.abspath(ROOT))

from data_gen.generate_events import main as generate
from database.load_raw import load_raw_events
from database.db import get_connection, execute_sql_file, log_pipeline_run

SQL_DIR = os.path.join(ROOT, "sql")

SQL_FILES = [
    "01_stg_events.sql",
    "02_funnel.sql",
    "04a_user_summary.sql",
    "03_cohort.sql",
    "04b_retention_health.sql",
]

RAW_CSV = os.path.join(ROOT, "data", "raw_events.csv")


def run_pipeline() -> None:
    print("\n🚀 JOIS Pipeline Starting...\n")
    t_start = time.time()

    print("── Step 1: Generate Events ──")
    generate()

    with open(RAW_CSV, "r") as f:
        events_generated = sum(1 for _ in f) - 1

    print("\n── Step 2: Load Raw CSV → SQLite ──")
    load_raw_events()

    print("\n── Step 3: Run SQL Transformations ──")
    conn = get_connection()
    for sql_file in SQL_FILES:
        path = os.path.join(SQL_DIR, sql_file)
        execute_sql_file(conn, path)

    duration = time.time() - t_start
    events_loaded = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]

    log_pipeline_run(
        conn,
        events_generated=events_generated,
        events_loaded=events_loaded,
        duration_seconds=duration,
    )
    conn.close()

    print(f"\n✅ Pipeline Complete in {duration:.1f}s. jois.db is ready.\n")


if __name__ == "__main__":
    run_pipeline()