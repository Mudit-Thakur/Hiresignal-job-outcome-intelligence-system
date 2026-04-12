import sqlite3
import os

import sqlite3
import os

# Use /tmp on Streamlit Cloud (writable), local repo path everywhere else
_LOCAL = os.path.join(os.path.dirname(__file__), "..", "jois.db")
DB_PATH = "/tmp/jois.db" if os.environ.get("HOME") == "/home/appuser" else _LOCAL


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    _ensure_pipeline_log(conn)
    return conn


def _ensure_pipeline_log(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_log (
            run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at            TEXT    NOT NULL,
            events_generated  INTEGER,
            events_loaded     INTEGER,
            duration_seconds  REAL,
            status            TEXT DEFAULT 'success'
        )
    """)
    conn.commit()


def log_pipeline_run(
    conn: sqlite3.Connection,
    events_generated: int,
    events_loaded: int,
    duration_seconds: float,
    status: str = "success",
) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_log
            (run_at, events_generated, events_loaded, duration_seconds, status)
        VALUES (datetime('now'), ?, ?, ?, ?)
        """,
        (events_generated, events_loaded, round(duration_seconds, 2), status),
    )
    conn.commit()


def execute_sql_file(conn: sqlite3.Connection, path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()
    print(f"  ✅ Executed: {os.path.basename(path)}")