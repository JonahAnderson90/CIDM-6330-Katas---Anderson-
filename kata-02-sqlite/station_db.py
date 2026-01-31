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

    # Stretch: Create index on observations.station_id
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_observations_station_id ON observations(station_id)")
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

    def update_station(conn: sqlite3.Connection, station_id: str, name: str = None, state: str = None) -> None:
        """Update a station's name and/or state by station_id."""
        cursor = conn.cursor()
        if name is not None:
            cursor.execute("UPDATE stations SET name = ? WHERE station_id = ?", (name, station_id))
        if state is not None:
            cursor.execute("UPDATE stations SET state = ? WHERE station_id = ?", (state, station_id))
        conn.commit()

    def delete_station(conn: sqlite3.Connection, station_id: str) -> None:
        """Delete a station by station_id."""
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stations WHERE station_id = ?", (station_id,))
        conn.commit()

    def update_observation(conn: sqlite3.Connection, obs_id: int, temp_c: float = None, date: str = None) -> None:
        """Update an observation's temperature and/or date by obs_id."""
        cursor = conn.cursor()
        if temp_c is not None:
            cursor.execute("UPDATE observations SET temp_c = ? WHERE obs_id = ?", (temp_c, obs_id))
        if date is not None:
            cursor.execute("UPDATE observations SET date = ? WHERE obs_id = ?", (date, obs_id))
        conn.commit()

    def delete_observation(conn: sqlite3.Connection, obs_id: int) -> None:
        """Delete an observation by obs_id."""
        cursor = conn.cursor()
        cursor.execute("DELETE FROM observations WHERE obs_id = ?", (obs_id,))
        conn.commit()

    def explain_query_plan_for_join(conn: sqlite3.Connection) -> list:
        """Return EXPLAIN QUERY PLAN output for the join query."""
        cursor = conn.cursor()
        cursor.execute("""
            EXPLAIN QUERY PLAN
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
