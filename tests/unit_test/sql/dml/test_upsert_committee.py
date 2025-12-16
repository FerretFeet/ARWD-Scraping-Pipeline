from datetime import date

import pytest
from psycopg.rows import dict_row


@pytest.fixture
def sql_file():
    return "dml/functions/upsert_committee.sql"
# Helper function to fetch all committee info records for easy inspection
def fetch_committee_info(db, committee_id, committee_name):
    """Fetches all records for a given committee ID and Name combination."""
    db.row_factory = dict_row
    db.execute(
        """
        SELECT committee_info_id, fk_committee_id, committee_name, url, start_date, end_date
        FROM committee_info
        WHERE fk_committee_id = %s AND committee_name = %s
        ORDER BY start_date;
        """,
        (committee_id, committee_name),
    )
    return db.fetchall()




@pytest.fixture
def setup_session_data(db):
    """Inserts necessary session data before running tests."""
    db.execute(
        """
        INSERT INTO sessions (session_code, session_name, start_date) VALUES
                                                                          ('S1', '2020 Session', '2020-01-01'),
                                                                          ('S2', '2021 Session', '2021-01-01'),
                                                                          ('S3', '2022 Session', '2022-01-01');
        """,
    )
    return True # Return value is arbitrary, mainly used for injection


class TestCommitteeInfoSCD2NewSchema:
    sql_file = "upsert_committee.sql"


    # --- Test 1: Initial Insert ---
    def test_initial_insert(self, db, setup_session_data):
        """Tests the first insert for a new (ID, Name) pair."""
        comm_id = 101
        name = "Budget Committee"
        url = "http://budget.com/v1"
        session = "S1"

        info_id = db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)"
            "AS committee_info_id",
            (comm_id, name, url, session),
        ).fetchone()["committee_info_id"]
        assert info_id is not None


        # Assertions
        records = fetch_committee_info(db, comm_id, name)
        assert len(records) == 1

        record = records[0]
        assert record["committee_info_id"] == 1
        assert record["fk_committee_id"] == comm_id
        assert record["committee_name"] == name
        assert record["url"] == url
        assert record["start_date"] == date(2020, 1, 1)
        assert record["end_date"] is None

    # --- Test 2: SCD2 Change (URL updated) ---
    def test_scd2_url_change(self, db, setup_session_data):
        """Tests that a change in URL closes the old record and inserts a new one."""
        comm_id = 202
        name = "Transportation Committee"
        session_1 = "S1"
        url_1 = "http://trans.com/old"
        session_2 = "S2"
        url_2 = "http://trans.com/new"

        db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)",
            (comm_id, name, url_1, session_1),
        )

        # 2. Second Insert with URL change (S2, URL v2)
        returned_static_id = db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s) AS committee_id",
            (comm_id, name, url_2, session_2),
        ).fetchone()["committee_id"]

        # ASSERTION 1: Verify the function returned the correct static ID
        assert returned_static_id == comm_id

        # Assertions
        records = fetch_committee_info(db, comm_id, name)
        assert len(records) == 2

        # Check Old Record (S1) - (No change needed here)
        old_record = records[0]
        assert old_record["url"] == url_1
        assert old_record["start_date"] == date(2020, 1, 1)
        assert old_record["end_date"] == date(2020, 12, 31)

        # Check New Record (S2)
        new_record = records[1]
        # ASSERTION 2: Use the newly retrieved record's ID for the assertion
        assert new_record["url"] == url_2
        assert new_record["start_date"] == date(2021, 1, 1)
        assert new_record["end_date"] is None


    def test_redundant_insert_no_change(self, db, setup_session_data):
        """Tests that calling the function again with identical data reuses the existing record."""
        comm_id = 303
        name = "Rules Committee"
        url = "http://rules.com"
        session = "S2"

        # 1. Initial Insert
        first_id = db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s) AS committee_id",
            (comm_id, name, url, session),
        ).fetchone()["committee_id"]

        # 2. Redundant Call (same ID, name, url, session/start_date)
        second_id = db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s) AS committee_id",
            (comm_id, name, url, session),
        ).fetchone()["committee_id"]

        # Assertions
        records = fetch_committee_info(db, comm_id, name)
        # Should only be one record
        assert len(records) == 1
        # The returned ID should be the ID of the existing record
        assert first_id == second_id


    def test_insert_session_change_same_data_no_change(self, db, setup_session_data):
        """Tests that calling the function again with only a different session reuses the existing record."""
        comm_id = 303
        name = "Rules Committee"
        url = "http://rules.com"
        session1 = "S1"
        session2 = "S2"

        # 1. Initial Insert
        first_id = db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s) AS committee_id",
            (comm_id, name, url, session1),
        ).fetchone()["committee_id"]

        # 2. Redundant Call (same ID, name, url, session/start_date)
        second_id = db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s) AS committee_id",
            (comm_id, name, url, session2),
        ).fetchone()["committee_id"]

        # Assertions
        records = fetch_committee_info(db, comm_id, name)
        # Should only be one record
        assert len(records) == 1
        # The returned ID should be the ID of the existing record
        assert first_id == second_id


    # --- Test 5: Handling Name Change (New History Required) ---
    def test_name_change_creates_new_history(self, db, setup_session_data):
        """
        Tests that changing the committee_name (even with the same ID)
        starts a brand new SCD2 history for the new name.
        """
        comm_id = 505

        # 1. Insert under OLD name (S1)
        old_name = "Original Name"
        db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)",
            (comm_id, old_name, "http://original.com", "S1"),
        )

        # 2. Insert under NEW name (S2)
        new_name = "Renamed Committee"
        db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)",
            (comm_id, new_name, "http://renamed.com", "S2"),
        )

        # Assertions

        # History for OLD name should have only ONE record and it should NOT be closed
        # (Since the new record uses a different name, it never looks up and closes the old name's record)
        old_records = fetch_committee_info(db, comm_id, old_name)
        assert len(old_records) == 1
        assert old_records[0]["end_date"] is None
        assert old_records[0]["start_date"] == date(2020, 1, 1)

        # History for NEW name should be a brand new active record
        new_records = fetch_committee_info(db, comm_id, new_name)
        assert len(new_records) == 1
        assert new_records[0]["end_date"] is None
        assert new_records[0]["start_date"] == date(2021, 1, 1)


    # --- Test 6: Invalid Session Code (Error Handling) ---
    def test_invalid_session_code_raises_exception(self, db, setup_session_data):
        """Tests that the function raises an exception if the session_code is not found."""
        comm_id = 606
        name = "Agriculture"
        invalid_session = "S99"

        # The exception type may vary based on your psycopg version, but Exception is safe
        with pytest.raises(Exception, match="Session code S99 not found"):
            db.execute(
                "SELECT upsert_committee(%s, %s, %s, %s)",
                (comm_id, name, "http://ag.com", invalid_session),
            )
        db.connection.rollback()
        # Ensure no records were inserted
        records = fetch_committee_info(db, comm_id, name)
        assert len(records) == 0

    # --- Test 7: Handling URL change and immediate return to original URL ---
    def test_scd2_cycle(self, db, setup_session_data):
        """Tests a sequence of changes to ensure multiple historical slices are created."""
        comm_id = 707
        name = "Veteran Affairs"
        session_1 = "S1"
        url_1 = "http://va.com/v1"
        session_2 = "S2"
        url_2 = "http://va.com/v2"
        session_3 = "S3"

        # 1. Initial Insert (S1, URL v1)
        db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)",
            (comm_id, name, url_1, session_1),
        )

        # 2. Change (S2, URL v2)
        db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)",
            (comm_id, name, url_2, session_2),
        )

        # 3. Change (S3, URL v1 - back to original)
        db.execute(
            "SELECT upsert_committee(%s, %s, %s, %s)",
            (comm_id, name, url_1, session_3),
        )

        # Assertions
        records = fetch_committee_info(db, comm_id, name)
        assert len(records) == 3

        # Record 1 (S1, URL v1)
        assert records[0]["start_date"] == date(2020, 1, 1)
        # Closed by S2 start date (2021-01-01 - 1 day)
        assert records[0]["end_date"] == date(2020, 12, 31)

        # Record 2 (S2, URL v2)
        assert records[1]["start_date"] == date(2021, 1, 1)
        # Closed by S3 start date (2022-01-01 - 1 day)
        assert records[1]["end_date"] == date(2021, 12, 31)

        # Record 3 (S3, URL v1)
        assert records[2]["start_date"] == date(2022, 1, 1)
        assert records[2]["url"] == url_1
        assert records[2]["end_date"] is None
