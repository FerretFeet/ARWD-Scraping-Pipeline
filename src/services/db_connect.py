"""Generator for a psycopg3 database connection."""

import os
import time
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
DB_HOST = os.getenv("PG_HOST", "DB_HOST")
DB_PORT = os.getenv("PG_PORT", "DB_PORT")
DB_SCHEMA = os.getenv("SCRAPER_SCHEMA", "public")  # default to public if not set


@contextmanager
def db_conn() -> Generator:
    """Waits for Postgres to be ready before yielding a connection."""
    timeout = 30  # seconds
    start = time.time()
    while True:
        try:
            conn = psycopg.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
            )
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {DB_SCHEMA};")
            break
        except psycopg.OperationalError as e:
            if time.time() - start > timeout:
                raise RuntimeError("Could not connect to Postgres within timeout") from e
            print("Waiting for Postgres to be ready...")
            time.sleep(1)

    try:
        yield conn
    finally:
        conn.close()