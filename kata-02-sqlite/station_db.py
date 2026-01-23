import sqlite3
from pathlib import Path
from typing import List, Tuple

DB_PATH = Path("kata-02-sqlite/stations.db")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create and return a SQLite connection."""
    return sqlite3.connect(db_path)


def create_tables(conn: sqlite3.Connection) -> None:
    """Create stations and observations tables."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            obs_id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            date TEXT NOT NULL,
            temp_c REAL NOT NULL,
            FOREIGN KEY (station_id) REFERENCES stations(station_id)
        )
    """)

    conn.commit()
def seed_stations(conn: sqlite3.Connection) -> None:
    """Insert sample stations."""
    cursor = conn.cursor()
    stations = [
        ("AMA001", "Amarillo North", "TX"),
        ("AMA002", "Amarillo South", "TX"),
        ("DEN001", "Denver Central", "CO"),
    ]

    cursor.executemany(
        "INSERT OR IGNORE INTO stations (station_id, name, state) VALUES (?, ?, ?)",
        stations,
    )
    conn.commit()


def seed_observations(conn: sqlite3.Connection) -> None:
    """Insert sample observations."""
    cursor = conn.cursor()
    observations = [
        ("AMA001", "2026-01-20", 2.1),
        ("AMA001", "2026-01-21", 4.0),
        ("AMA002", "2026-01-21", -1.2),
        ("DEN001", "2026-01-21", 7.3),
    ]

    cursor.executemany(
        "INSERT INTO observations (station_id, date, temp_c) VALUES (?, ?, ?)",
        observations,
    )
    conn.commit()
def get_station_observations(conn: sqlite3.Connection) -> List[Tuple]:
    """Return joined station + observation data."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            s.station_id,
            s.name,
            o.date,
            o.temp_c
        FROM stations s
        JOIN observations o ON s.station_id = o.station_id
        ORDER BY o.date
    """)
    return cursor.fetchall()
