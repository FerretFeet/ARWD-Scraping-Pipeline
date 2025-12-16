
import pytest

from tests.conftest import SQL_DIR

# SQL_DIR = project_root / "sql"


@pytest.fixture(autouse=True)
def db(db_engine, sql_file):
    """
    Runs each test in a transaction and rolls back after the test.
    """
    db_engine.autocommit = False
    with db_engine.cursor() as cur:
        fp = SQL_DIR / sql_file
        with open(fp) as f:
            cur.execute(f.read())

        yield cur

        db_engine.rollback()
