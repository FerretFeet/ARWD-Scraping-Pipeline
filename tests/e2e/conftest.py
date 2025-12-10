
import pytest

# SQL_DIR = project_root / "sql"


@pytest.fixture(scope="function", autouse=True)
def test_db(db_engine):
    """
    Runs each test in a transaction and rolls back after the test.
    """
    # Start a transaction
    db_engine.autocommit = False
    yield db_engine
    # with db_engine.cursor() as cur:
    #     yield cur  # cursor available to test
    #     # Rollback everything after the test
    db_engine.rollback()
