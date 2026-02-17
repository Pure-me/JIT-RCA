from __future__ import annotations
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / 'data' / 'jit.db'

SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS deliveries (
    uid TEXT PRIMARY KEY,
    cnr_tour TEXT,
    cnr_cust TEXT,
    nm_short_unload TEXT,
    customer TEXT,
    site TEXT,
    planned_time TEXT,
    actual_time TEXT,
    date_dos TEXT,
    raw_planned TEXT,
    raw_actual TEXT,
    payload_json TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_deliveries_date ON deliveries(date_dos);
CREATE INDEX IF NOT EXISTS idx_deliveries_tour ON deliveries(cnr_tour);
'''
def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA_SQL)

def insert_many(rows: list[tuple]):
    sql = '''INSERT OR IGNORE INTO deliveries
    (uid, cnr_tour, cnr_cust, nm_short_unload, customer, site, planned_time, actual_time, date_dos, raw_planned, raw_actual, payload_json)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''
    with get_conn() as conn:
        conn.executemany(sql, rows)
        conn.commit()

def query_between(date_from: str, date_to: str) -> list[dict]:
    sql = 'SELECT * FROM deliveries WHERE date_dos >= ? AND date_dos < ?'
    with get_conn() as conn:
        cur = conn.execute(sql, (date_from, date_to))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
