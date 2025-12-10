from datetime import date
from pathlib import Path

import psycopg
import pytest

from src.data_pipeline.load.pipeline_loader import PipelineLoader


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
@pytest.fixture
def sql_file():
    return "dml/upsert_bill_with_sponsors.sql"


SQL_FILE = Path("sql/dml/upsert_bill_with_sponsors.sql")


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def clean_tables(db):
    """Reset all tables before each test."""
    db.execute("""
        TRUNCATE legislator_votes, vote_events, sponsors, committee_membership,
                 committees, legislator_history, legislators, bill_documents,
                 bills, sessions
        RESTART IDENTITY CASCADE;
    """)
    db.execute("COMMIT;")


@pytest.fixture
def setup_session(db):
    db.execute("""
        INSERT INTO sessions (session_code, name, start_date)
        VALUES ('2025A', '2025 Session', '2025-01-01')
        ON CONFLICT (session_code) DO NOTHING
        RETURNING session_code;
    """)
    return db.fetchone()["session_code"]


@pytest.fixture
def setup_legislators(db):
    ids = []
    for first, last, phone, email, addr in [
        ("Alice", "Jones", "111", "a@a.com", "Addr 1"),
        ("Bob", "Smith", "222", "b@b.com", "Addr 2"),
        ("Carol", "Lee", "333", "c@c.com", "Addr 3"),
    ]:
        db.execute("""
            INSERT INTO legislators (first_name, last_name, phone, email, address)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING legislator_id;
        """, (first, last, phone, email, addr))
        ids.append(db.fetchone()["legislator_id"])
    return ids


@pytest.fixture
def setup_committees(db):
    ids = []
    for name in ["Finance", "Education"]:
        db.execute("""
            INSERT INTO committees (name)
            VALUES (%s)
            RETURNING committee_id;
        """, (name,))
        ids.append(db.fetchone()["committee_id"])
    return ids


@pytest.fixture
def sample_bill_data(setup_session, setup_legislators, setup_committees):
    return {
        "title": "Test Bill",
        "bill_no": "B-001",
        "url": "http://example.com/bill",
        "session_code": setup_session,
        "intro_date": date(2025, 1, 1),
        "act_date": date(2025, 2, 1),
        "bill_documents": {
            "amendments": ["http://example.com/amend1"],
            "bill_text": ["http://example.com/bill_text"],
        },
        "lead_sponsor": {"legislator_id": [setup_legislators[0]]},
        "other_primary_sponsor": {"committee_id": [setup_committees[0]]},
        "cosponsors": {
            "legislator_id": [setup_legislators[1]],
            "committee_id": [setup_committees[1]],
        },
    }


@pytest.fixture
def loader(sql_file=SQL_FILE):
    """Create a PipelineLoader instance for upsert_bill_with_sponsors."""
    return PipelineLoader(
        sql_file_path=sql_file,
        upsert_function_name="upsert_bill_with_sponsors",
        required_params={
            "title": str,
            "bill_no": str,
            "url": str,
            "session_code": str,
            "intro_date": date,
            "act_date": date,
            "bill_documents": dict,
            "lead_sponsor": dict,
            "other_primary_sponsor": dict,
            "cosponsors": dict,
        },
        insert="SELECT upsert_bill_with_sponsors("
               "p_title := %(p_title)s, "
               "p_bill_no := %(p_bill_no)s, "
               "p_url := %(p_url)s, "
               "p_session_code := %(p_session_code)s, "
               "p_intro_date := %(p_intro_date)s, "
               "p_act_date := %(p_act_date)s, "
               "p_bill_documents := %(p_bill_documents)s::jsonb, "
               "p_lead_sponsor := %(p_lead_sponsor)s::jsonb, "
               "p_other_primary_sponsor := %(p_other_primary_sponsor)s::jsonb, "
               "p_cosponsors := %(p_cosponsors)s::jsonb);",
    )


# ----------------------------------------------------------------------
# Extended Tests
# ----------------------------------------------------------------------

class TestPipelineLoaderEdgeCases:

    def test_insert_new_bill(self, db, loader, sample_bill_data):
        result = loader.execute(sample_bill_data, db)
        assert result is not None

    def test_idempotent_double_insert(self, db, loader, sample_bill_data):
        loader.execute(sample_bill_data, db)
        loader.execute(sample_bill_data, db)
        db.execute("SELECT COUNT(*) FROM bills;")
        assert db.fetchone()["count"] == 1

    def test_update_bill_title(self, db, loader, sample_bill_data):
        original = sample_bill_data.copy()
        original["title"] = "Original Title"
        loader.execute(original, db)

        updated = sample_bill_data.copy()
        updated["title"] = "Updated Title"
        loader.execute(updated, db)

        db.execute("SELECT title FROM bills WHERE bill_no = %(p_bill_no)s;",
                   {"p_bill_no": sample_bill_data["bill_no"]})
        assert db.fetchone()["title"] == "Updated Title"

    def test_missing_required_parameter_strict(self, loader, db):
        loader.strict = True
        incomplete_data = {
            "title": "Missing Bill No",
            "url": "http://example.com",
        }
        with pytest.raises(ValueError):
            loader.execute(incomplete_data, db)

    def test_missing_required_parameter_non_strict(self, loader, db):
        loader.strict = False
        incomplete_data = {
            "title": "Missing Bill No",
            "url": "http://example.com",
        }
        # Should log warning but not raise
        assert loader.validate_input(incomplete_data) is False

    def test_empty_json_fields(self, db, loader, sample_bill_data):
        sample_bill_data["bill_documents"] = {}
        sample_bill_data["lead_sponsor"] = {}
        sample_bill_data["cosponsors"] = {}
        sample_bill_data["other_primary_sponsor"] = {}
        loader.execute(sample_bill_data, db)

        db.execute("SELECT COUNT(*) FROM bills;")
        assert db.fetchone()["count"] == 1
        db.execute("SELECT COUNT(*) FROM sponsors;")
        assert db.fetchone()["count"] == 0  # no sponsors added

    def test_partial_sponsor_data(self, db, loader, sample_bill_data):
        # Remove one cosponsor, keep lead sponsor only
        sample_bill_data["cosponsors"] = {}
        loader.execute(sample_bill_data, db)
        db.execute("SELECT COUNT(*) FROM sponsors;")
        assert db.fetchone()["count"] == 2  # lead + primary committee only

    def test_multiple_bills_with_same_session(self, db, loader, sample_bill_data):
        # Insert two bills in same session
        loader.execute(sample_bill_data, db)
        second_bill = sample_bill_data.copy()
        second_bill["bill_no"] = "B-002"
        second_bill["title"] = "Second Bill"
        loader.execute(second_bill, db)

        db.execute("SELECT COUNT(*) FROM bills;")
        assert db.fetchone()["count"] == 2

    def test_insert_invalid_date_type(self, db, loader, sample_bill_data):
        invalid_data = sample_bill_data.copy()
        invalid_data["intro_date"] = "not-a-date"
        with pytest.raises(psycopg.errors.InvalidDatetimeFormat):
            loader.execute(invalid_data, db)
