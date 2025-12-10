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
    return "dml/upsert_legislator.sql"

@pytest.fixture
def sql_file_path():
    return Path("sql/dml/upsert_legislator.sql")


@pytest.fixture
def loader(sql_file_path):
    return PipelineLoader(
        sql_file_path=sql_file_path,
        upsert_function_name="upsert_legislator",
        required_params={
            "first_name": str,
            "last_name": str,
            "url": str,
            "phone": str,
            "email": str,
            "address": str,
            "district": str,
            "seniority": int,
            "chamber": str,
            "party": str,
            "start_date": date,
            "committee_ids": list,
        },
        insert="SELECT upsert_legislator("
               "p_first_name := %(p_first_name)s, "
               "p_last_name := %(p_last_name)s, "
               "p_url := %(p_url)s, "
               "p_phone := %(p_phone)s, "
               "p_email := %(p_email)s, "
               "p_address := %(p_address)s, "
               "p_district := %(p_district)s, "
               "p_seniority := %(p_seniority)s, "
               "p_chamber := %(p_chamber)s, "
               "p_party := %(p_party)s, "
               "p_start_date := %(p_start_date)s, "
               "p_committee_ids := %(p_committee_ids)s);",
    )


# --------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------
def _insert_committee(db, name, url=None):
    db.execute("""
        INSERT INTO committees (name, url)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE
        SET url = COALESCE(EXCLUDED.url, committees.url)
        RETURNING committee_id
    """, (name, url))
    return db.fetchone()["committee_id"]


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
        "party": "D",
        "start_date": date(2020, 1, 1),
        "committee_ids": [],
    }
    data.update(overrides)
    return data


# ===========================================================================
# Test Suite
# ===========================================================================
class TestPipelineLoaderUpsertLegislator:

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
        data2 = _default_legislator_data(first_name="Dana", last_name="Ray", district="2", start_date=date(2021, 1, 1))
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_triggers_on_party_change(self, db, loader):
        data1 = _default_legislator_data(first_name="Eve", last_name="Adams", party="D")
        data2 = _default_legislator_data(first_name="Eve", last_name="Adams", party="R", start_date=date(2021,1,1))
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_triggers_on_chamber_change(self, db, loader):
        data1 = _default_legislator_data(first_name="Gail", last_name="Ivy", chamber="house")
        data2 = _default_legislator_data(first_name="Gail", last_name="Ivy", chamber="senate", start_date=date(2020,1,1))
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    # ----------------------------------------------------------------------
    # 5. Committee memberships
    # ----------------------------------------------------------------------
    def test_insert_new_committees_on_first_upsert(self, db, loader):
        c1 = _insert_committee(db, "Agriculture")
        c2 = _insert_committee(db, "Budget")
        data = _default_legislator_data(first_name="Katie", last_name="Lee", committee_ids=[c1, c2])
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 2

    def test_re_running_upsert_does_not_duplicate_memberships(self, db, loader):
        c1 = _insert_committee(db, "Agriculture-B")
        data = _default_legislator_data(first_name="Sam", last_name="Wise", committee_ids=[c1])
        loader.execute(data, db)
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_existing_memberships_are_closed_if_removed_from_list(self, db, loader):
        c1 = _insert_committee(db, "Finance")
        c2 = _insert_committee(db, "Judiciary")
        data1 = _default_legislator_data(first_name="Sync", last_name="Test", committee_ids=[c1, c2])
        data2 = _default_legislator_data(first_name="Sync", last_name="Test", start_date=date(2021,1,1), committee_ids=[c1])
        loader.execute(data1, db)
        loader.execute(data2, db)

        # Judiciary should be closed
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c2,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

        # Finance old membership still open
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2020-01-01';", (c1,))
        assert db.fetchone()["membership_end"] is None

        # Finance new membership (if inserted) is open
        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2021-01-01';", (c1,))
        row = db.fetchone()
        if row:
            assert row["membership_end"] is None

    def test_committee_membership_array_null(self, db, loader):
        c1 = _insert_committee(db, "Finance-Null")
        data1 = _default_legislator_data(first_name="Null", last_name="Committee", committee_ids=[c1])
        loader.execute(data1, db)
        data2 = _default_legislator_data(first_name="Null", last_name="Committee", start_date=date(2021,1,1), committee_ids=None)
        loader.execute(data2, db)

        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c1,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

    def test_committee_membership_array_empty(self, db, loader):
        c1 = _insert_committee(db, "Finance-Empty")
        data1 = _default_legislator_data(first_name="Empty", last_name="Committee", committee_ids=[c1])
        loader.execute(data1, db)
        data2 = _default_legislator_data(first_name="Empty", last_name="Committee", start_date=date(2021,1,1), committee_ids=[])
        loader.execute(data2, db)

        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s;", (c1,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

    def test_multiple_committee_memberships_added_and_removed(self, db, loader):
        c1 = _insert_committee(db, "C1")
        c2 = _insert_committee(db, "C2")
        c3 = _insert_committee(db, "C3")
        data1 = _default_legislator_data(first_name="Multi", last_name="Committee", committee_ids=[c1, c2, c3])
        data2 = _default_legislator_data(first_name="Multi", last_name="Committee", start_date=date(2021,1,1), committee_ids=[c1, c3])
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
        c1 = _insert_committee(db, "DupC")
        data = _default_legislator_data(first_name="Dup", last_name="Committee", committee_ids=[c1])
        loader.execute(data, db)
        loader.execute(data, db)

        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_committee_duplicates_in_array(self, db, loader):
        c1 = _insert_committee(db, "C-Dup")
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
        data2 = _default_legislator_data(first_name="URL", last_name="Test", url="new_url", start_date=date(2021,1,1))
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

    def test_history_and_committee_change_simultaneously(self, db, loader):
        c1 = _insert_committee(db, "Combo1")
        c2 = _insert_committee(db, "Combo2")
        data1 = _default_legislator_data(first_name="Combo", last_name="Test", committee_ids=[c1])
        data2 = _default_legislator_data(first_name="Combo", last_name="Test", party="R", start_date=date(2021,1,1), committee_ids=[c1, c2])
        loader.execute(data1, db)
        loader.execute(data2, db)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

        db.execute("SELECT membership_end FROM committee_membership WHERE fk_committee_id=%s AND membership_start='2021-01-01';", (c2,))
        assert db.fetchone()["membership_end"] is None
