"""Function to insert the starting sessions and dates into the database."""

import json
from pathlib import Path

import psycopg

from src.utils.paths import project_root

session_dict_path = project_root / "sessions_dict.json"
session_insert_path = project_root / "sql" / "dml" / "functions" / "insert_session.sql"


with Path.open(session_dict_path) as f:
    sessions_data = json.load(f)

with Path.open(session_insert_path) as f:
    sql_function = f.read()


def insert_sessions(conn: psycopg.Connection, data: dict, sql_func: str) -> list:
    """Insert known sessions into the database."""
    with conn.cursor() as cur:
        cur.execute(sql_func)

        for code, info in data.items():
            cur.execute(
                "SELECT insert_session(%s, %s, %s);",
                (code, info["label"], info["year"]),
            )
        conn.commit()
        cur.execute("SELECT session_code FROM sessions ORDER BY start_date ASC;")
        return cur.fetchall()


if __name__ == "__main__":
    with psycopg.connect("dbname=yourdb user=youruser password=yourpass host=localhost") as conn:
        insert_sessions(conn, sessions_data, sql_function)
