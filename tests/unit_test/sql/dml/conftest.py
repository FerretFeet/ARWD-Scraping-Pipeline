
import pytest

from tests.unit_test.sql.conftest import SQL_DIR

# SQL_DIR = project_root / "sql"


@pytest.fixture(autouse=True)
def db(db_engine, sql_file):
    """
    Runs each test inside a fresh transaction and rolls back.
    """
    db_engine.autocommit = False
    cur = db_engine.cursor()
    fp = SQL_DIR / sql_file
    # Load the SQL file for the function
    with open(fp) as f:
        cur.execute(f.read())

    yield cur

    # Roll back everything the test did
    db_engine.rollback()
    cur.close()
