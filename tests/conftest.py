import json
import os
from pathlib import Path
from unittest.mock import patch

import psycopg
import pytest
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg.rows import dict_row

from src.data_pipeline.extract.webcrawler import Crawler
from src.utils.paths import project_root


###########################################
# RETURN OR DOWNLOAD HTML FIXTURE
###########################################
def pytest_addoption(parser):
    parser.addoption(
        "--refresh-html-fixtures",
        action="store_true",
        default=False,
        help="Download missing HTML fixtures if they are not present locally.",
    )


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def download_fixture(url: str, path: Path):
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    path.write_text(resp.text, encoding="utf-8")
    return path


@pytest.fixture
def html_selector_fixture(request):
    """Takes params in this shape:

        (name, url, variant)

    and returns:

        {
            "soup": BeautifulSoup,
            "path": Path,
            "filename": str,
            "url": url,
            "variant": variant
        }

    Also patches Crawler.get_page to return soup.
    """
    name, url, variant = request.param
    force_refresh = request.config.getoption("--refresh-html-fixtures")

    # Example: name="bill_page/bill", variant="v1"
    filename = f"html/{name}.{variant}.html"
    fp = FIXTURE_DIR / filename

    # Download if needed
    if force_refresh or not fp.exists():
        fp.parent.mkdir(parents=True, exist_ok=True)
        download_fixture(url, fp)

    # Load into BeautifulSoup
    html_content = fp.read_text(encoding="utf-8")
    soup = BeautifulSoup(html_content, "html.parser")

    with patch.object(Crawler, "get_page", return_value=soup):
        yield {
            "soup": soup,
            "path": fp,
            "filename": filename,
            "url": url,
            "variant": variant,
            "html": html_content,
        }


######################################
# Return Transformation Input
######################################
def load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)






load_dotenv(project_root / ".env")

SQL_DIR = project_root / "sql"
DB_NAME = os.environ.get("DB_NAME")
TEST_DB_NAME = os.getenv("TEST_DB_NAME")
TEST_DB_USER = os.getenv("TEST_DB_USER")
TEST_DB_PASS = os.getenv("TEST_DB_PASS")
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS = os.getenv("ADMIN_PASS")
SCHEMA = os.getenv("SCRAPER_SCHEMA")


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
            sql_text = open(sql_path).read()
            sql_text = sql_text.replace(":SCRAPER_SCHEMA", SCHEMA)
            cur.execute(sql_text)
    yield conn  # tests use scraper_test

    # 3. Drop test DB after session
    conn.close()
    with psycopg.connect(f"dbname={DB_NAME} user={ADMIN_USER} password={ADMIN_PASS} host=127.0.0.1") as admin_conn:
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB_NAME}")
