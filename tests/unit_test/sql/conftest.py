import os

import psycopg
import pytest
from dotenv import load_dotenv
from psycopg.rows import dict_row

from src.utils.paths import project_root

load_dotenv(project_root / ".env")

SQL_DIR = project_root / "sql"
DB_NAME = os.environ.get("DB_NAME")
TEST_DB_NAME = os.getenv("TEST_DB_NAME")
TEST_DB_USER = os.getenv("TEST_DB_USER")
TEST_DB_PASS = os.getenv("TEST_DB_PASS")
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS = os.getenv("ADMIN_PASS")


sql_folder = project_root / "sql"

DB_INIT_FILES = [sql_folder / "ddl" / "enums.sql",
                 sql_folder / "ddl" / "tables.sql"]

@pytest.fixture(scope="session")
def db_engine():
    # 1. Connect as admin to create test DB
    with psycopg.connect(
        dbname=DB_NAME,
        user=ADMIN_USER,
        password=ADMIN_PASS,
        host="127.0.0.1",
        row_factory=dict_row,
    ) as admin_conn:
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
            cur.execute(f"CREATE DATABASE {TEST_DB_NAME} OWNER {TEST_DB_USER}")

    # 2. Connect as scraper_test to initialize schema
    conn = psycopg.connect(
        dbname=TEST_DB_NAME,
        user=TEST_DB_USER,
        password=TEST_DB_PASS,
        host="127.0.0.1",
        row_factory=dict_row,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        for sql_file in DB_INIT_FILES:
            sql_path = SQL_DIR / sql_file  # adjust path
            cur.execute(open(sql_path).read())

    yield conn  # tests use scraper_test

    # 3. Drop test DB after session
    conn.close()
    with psycopg.connect(f"dbname={os.environ['DB_NAME']} user={ADMIN_USER} password={ADMIN_PASS} host=127.0.0.1") as admin_conn:
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
