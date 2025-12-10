# db_connect.py
import os
from contextlib import contextmanager

import psycopg
from dotenv import load_dotenv

from src.utils.paths import project_root

load_dotenv(project_root / ".env")

# Optional: load credentials from environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("SCRAPER_USER")
DB_PASSWORD = os.getenv("SCRAPER_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SCHEMA = os.getenv("SCRAPER_SCHEMA", "public")  # default to public if not set
assert DB_NAME is not None
assert DB_USER is not None
assert DB_PASSWORD is not None
assert DB_HOST is not None
assert DB_PORT is not None
assert DB_SCHEMA is not None

@contextmanager
def db_conn():
    """
    Context manager that yields a psycopg3 connection.

    Usage:
        with db_conn() as conn:
            # use conn here
    """
    conn = psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA};")
        yield conn
    finally:
        conn.close()
