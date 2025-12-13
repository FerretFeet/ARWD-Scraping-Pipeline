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
    return "dml/functions/upsert_legislator.sql"

@pytest.fixture
def sql_file_path():
    return Path("sql/dml/functions/upsert_legislator.sql")

@pytest.fixture
def loader(sql_file_path):
    return PipelineLoader(
        sql_file_path=sql_file_path,
        upsert_function_name="Upsert Legislator",
        required_params={
            "first_name": str,
            "last_name": str,
            "url": str,
            "district": str,
            "seniority": int,
            "chamber": str,
            "session_code": str,
        },
        insert="""SELECT upsert_legislator(
               p_first_name := %(p_first_name)s,
               p_last_name := %(p_last_name)s,
               p_url := %(p_url)s,
               p_phone := %(p_phone)s,
               p_email := %(p_email)s,
               p_address := %(p_address)s,
               p_district := %(p_district)s,
               p_seniority := %(p_seniority)s,
               p_chamber := %(p_chamber)s,
               p_party := %(p_party)s,
               p_session_code := %(p_session_code)s,
               p_committee_ids := %(p_committee_ids)s::int[]
               ) AS legislator_id;""",
    )

# --------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------

def _insert_session(db, code, name, start_date):
    """Helper to ensure session exists for lookup."""
    db.execute("""
        INSERT INTO sessions (session_code, session_name, start_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (session_code) DO NOTHING
    """, (code, name, start_date))

def _insert_committee(db, comm_id):
    db.execute("""
        INSERT INTO committees (committee_id)
        VALUES (%s)
        ON CONFLICT (committee_id) DO NOTHING;
    """, (comm_id,))
    return comm_id

def _default_legislator_data(**overrides):
    data = {
        "first_name": "Test",
        "last_name": "Legislator",
        "url": "url",
        "phone": "111",
        "email": "a@b.com",
        "address": "Addr",
        "district": "1",
        "seniority": 1,
        "chamber": "house",
        "party": "d",
        "session_code": "2020-REG",  # CHANGED: Default session
        "committee_ids": [],
    }
    data.update(overrides)
    return data

# ===========================================================================
# Test Suite
# ===========================================================================
class TestPipelineLoaderUpsertLegislator:

    # ----------------------------------------------------------------------
    # Setup: Ensure sessions exist for start_date lookups
    # ----------------------------------------------------------------------
    @pytest.fixture(autouse=True)
    def setup_sessions(self, db):
        """Pre-populate sessions table so SQL lookups succeed."""
        _insert_session(db, "2020-REG", "2020", date(2020, 1, 1))
        _insert_session(db, "2021-REG", "2021", date(2021, 1, 1))

    # ----------------------------------------------------------------------
    # 1. Basic insert
    # ----------------------------------------------------------------------
    def test_insert_new_legislator_creates_history(self, db, loader):
        data = _default_legislator_data(first_name="Alice", last_name="Jones")
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 1
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 2. Identity resolution
    # ----------------------------------------------------------------------
    def test_does_not_duplicate_legislator_when_identity_matches(self, db, loader):
        data1 = _default_legislator_data(first_name="Bob", last_name="Smith")
        data2 = _default_legislator_data(first_name="Bob", last_name="Smith", phone="999", email="new@b")
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 3. SCD1 updates
    # ----------------------------------------------------------------------
    def test_scd1_updates_only_non_history_fields(self, db, loader):
        data1 = _default_legislator_data(first_name="Carl", last_name="Lee", phone="333", email="c@c")
        data2 = _default_legislator_data(first_name="Carl", last_name="Lee", phone="444", email="updated@c")
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 4. SCD2: changing district/party/chamber/url
    # ----------------------------------------------------------------------
    def test_scd2_creates_new_history_when_district_changes(self, db, loader):
        data1 = _default_legislator_data(first_name="Dana", last_name="Ray", district="1")
        # CHANGED: start_date=... -> session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Dana", last_name="Ray", district="2", session_code="2021-REG")
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_triggers_on_party_change(self, db, loader):
        data1 = _default_legislator_data(first_name="Eve", last_name="Adams", party="D")
        data2 = _default_legislator_data(first_name="Eve", last_name="Adams", party="R", session_code="2021-REG")
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_triggers_on_chamber_change(self, db, loader):
        data1 = _default_legislator_data(first_name="Gail", last_name="Ivy", chamber="house")
        # CHANGED: start_date=... -> session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Gail", last_name="Ivy", chamber="senate", session_code="2021-REG")
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    # ----------------------------------------------------------------------
    # 5. Committee memberships
    # ----------------------------------------------------------------------
    def test_insert_new_committees_on_first_upsert(self, db, loader):
        c1 = _insert_committee(db, 1)
        c2 = _insert_committee(db, 2)
        data = _default_legislator_data(first_name="Katie", last_name="Lee", committee_ids=[c1, c2])
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 2

    def test_re_running_upsert_does_not_duplicate_memberships(self, db, loader):
        c1 = _insert_committee(db, 1)
        data = _default_legislator_data(first_name="Sam", last_name="Wise", committee_ids=[c1])
        loader.execute(data, db)
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_existing_memberships_are_closed_if_removed_from_list(self, db, loader):
        c1 = _insert_committee(db, 1)
        c2 = _insert_committee(db, 2)
        data1 = _default_legislator_data(first_name="Sync", last_name="Test", committee_ids=[c1, c2])
        # CHANGED: session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Sync", last_name="Test", session_code="2021-REG", committee_ids=[c1])
        loader.execute(data1, db)
        loader.execute(data2, db)

        # Judiciary should be closed (2021-01-01 - 1 day = 2020-12-31)
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c2,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

        # Finance old membership still open (no end date)
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2020-01-01';", (c1,))
        assert db.fetchone()["membership_end"] is None

        # Finance new membership (if inserted) - logic dictates it shouldn't re-insert if open, so check basic existence
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2021-01-01';", (c1,))
        row = db.fetchone()
        if row:
            assert row["membership_end"] is None

    def test_committee_membership_array_null(self, db, loader):
        c1 = _insert_committee(db, 1)
        data1 = _default_legislator_data(first_name="Null", last_name="Committee", committee_ids=[c1])
        loader.execute(data1, db)
        # CHANGED: session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Null", last_name="Committee", session_code="2021-REG", committee_ids=None)
        loader.execute(data2, db)

        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c1,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

    def test_committee_membership_array_empty(self, db, loader):
        c1 = _insert_committee(db, 2)
        data1 = _default_legislator_data(first_name="Empty", last_name="Committee", committee_ids=[c1])
        loader.execute(data1, db)
        # CHANGED: session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Empty", last_name="Committee", session_code="2021-REG", committee_ids=[])
        loader.execute(data2, db)

        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c1,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

    def test_multiple_committee_memberships_added_and_removed(self, db, loader):
        c1 = _insert_committee(db, 1)
        c2 = _insert_committee(db, 2)
        c3 = _insert_committee(db, 3)
        data1 = _default_legislator_data(first_name="Multi", last_name="Committee", committee_ids=[c1, c2, c3])
        # CHANGED: session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Multi", last_name="Committee", session_code="2021-REG", committee_ids=[c1, c3])
        loader.execute(data1, db)
        loader.execute(data2, db)

        # Verify C2 closed
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c2,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

        # Verify C1 and C3 still open
        for cid in (c1, c3):
            db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2020-01-01';", (cid,))
            assert db.fetchone()["membership_end"] is None

    def test_committee_membership_duplicate_dates(self, db, loader):
        c1 = _insert_committee(db, 1)
        data = _default_legislator_data(first_name="Dup", last_name="Committee", committee_ids=[c1])
        loader.execute(data, db)
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_committee_duplicates_in_array(self, db, loader):
        c1 = _insert_committee(db, 2)
        data = _default_legislator_data(first_name="DupArray", last_name="Test", committee_ids=[c1, c1, c1])
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_committee_nonexistent_ids(self, db, loader):
        data = _default_legislator_data(first_name="Nonexistent", last_name="Test", committee_ids=[99999])
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            loader.execute(data, db)

    def test_history_triggers_on_url_change(self, db, loader):
        data1 = _default_legislator_data(first_name="URL", last_name="Test", url="old_url")
        # CHANGED: session_code="2021-REG"
        data2 = _default_legislator_data(first_name="URL", last_name="Test", url="new_url", session_code="2021-REG")
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_history_no_duplicate_on_same_data(self, db, loader):
        data = _default_legislator_data(first_name="Same", last_name="Test", url="url")
        loader.execute(data, db)
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    def test_history_no_update_if_only_session_change(self, db, loader):
        c1 = _insert_committee(db, 1)
        c2 = _insert_committee(db, 2)
        data1 = _default_legislator_data(first_name="Combo", last_name="Test", party="R",
                                         session_code="2020-REG", committee_ids=[c1, c2])
        data2 = _default_legislator_data(first_name="Combo", last_name="Test", party="R",
                                         session_code="2021-REG", committee_ids=[c1, c2])

        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1
        db.execute("SELECT end_date FROM legislator_history;")
        assert db.fetchone()["end_date"] is None

    def test_history_and_committee_change_simultaneously(self, db, loader):
        c1 = _insert_committee(db, 1)
        c2 = _insert_committee(db, 2)
        data1 = _default_legislator_data(first_name="Combo", last_name="Test", committee_ids=[c1])
        # CHANGED: session_code="2021-REG"
        data2 = _default_legislator_data(first_name="Combo", last_name="Test", party="R", session_code="2021-REG", committee_ids=[c1, c2])
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2021-01-01';", (c2,))
        assert db.fetchone()["membership_end"] is None
