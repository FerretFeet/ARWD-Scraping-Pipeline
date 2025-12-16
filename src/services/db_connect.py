"""Generator for a psycopg3 database connection."""

import os
from collections.abc import Generator
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


@contextmanager
def db_conn() -> Generator:
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
