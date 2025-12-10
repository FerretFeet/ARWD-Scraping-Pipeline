import pytest


@pytest.fixture
def sql_file():
    # This test wants only this SQL loaded
    return "dml/upsert_legislator.sql"

def _insert_legislator_history(db, legislator_id, district, seniority, chamber, party,
                               start_date, end_date=None):
    db.execute("""
        INSERT INTO legislator_history
            (fk_legislator_id, district, seniority, chamber, party, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (legislator_id, district, seniority, chamber, party, start_date, end_date))


def _insert_legislator(db, first, last, url, phone=None, email=None, address=None):
    db.execute("""
        INSERT INTO legislators (first_name, last_name, url, phone, email, address)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING legislator_id
    """, (first, last, url, phone, email, address))
    return db.fetchone()[0]


# ===========================================================================
# TEST SUITE
# ===========================================================================

class TestUpsertLegislator:
    # ----------------------------------------------------------------------
    # Helper Setup Function
    # ----------------------------------------------------------------------
    def _insert_committee(self, db, name, url=None):
        """Helper to insert/upsert a committee and return its ID."""
        db.execute(
            """
                   INSERT INTO committees (name, url)
                   VALUES (%s, %s)
                       ON CONFLICT (name) DO UPDATE SET
                       url = COALESCE(EXCLUDED.url, committees.url)
                                                 RETURNING committee_id;
                   """,
            (name, url),
        )
        return db.fetchone()["committee_id"]

    # ----------------------------------------------------------------------
    # 10.1 Insertion
    # ----------------------------------------------------------------------

    def test_insert_new_committees_on_first_upsert(self, db):
        # Setup: Create necessary committee IDs
        ag_id = self._insert_committee(db, "Agriculture")
        budget_id = self._insert_committee(db, "Budget")

        # Insert new legislator with memberships
        db.execute(
            """
                SELECT upsert_legislator(
                    'Katie', 'Lee', 'u10', '1', 'k@k', 'a',
                    '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s, %s]
                );
            """,
            (ag_id, budget_id),
        )

        # Assert: Two committee membership records exist
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 2

        # Assert: Check start date matches p_start_date
        db.execute("SELECT membership_start FROM committee_membership ORDER BY membership_start;")
        results = [str(r["membership_start"]) for r in db.fetchall()]
        assert results == ["2020-01-01", "2020-01-01"]

    def test_re_running_upsert_does_not_duplicate_memberships(self, db):
        ag_id = self._insert_committee(db, "Agriculture-B")

        # First Call
        db.execute(
            """
                SELECT upsert_legislator(
                    'Sam', 'Wise', 'u11', '1', 's@s', 'a',
                    '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s]
                );
            """,
            (ag_id,),
        )

        # Second Call (same data, same date)
        db.execute(
            """
                SELECT upsert_legislator(
                    'Sam', 'Wise', 'u11', '1', 's@s', 'a',
                    '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s]
                );
            """,
            (ag_id,),
        )

        # Assert: Only one membership record should exist
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 10.2 Syncing / Closing Logic (The Core SCD-2 for Memberships)
    # ----------------------------------------------------------------------

    def test_existing_memberships_are_closed_if_removed_from_list(self, db):
        # Setup: Insert Legislator
        leg_id = _insert_legislator(db, "Sync", "Test", "u12")
        finance_id = self._insert_committee(db, "Finance")
        judiciary_id = self._insert_committee(db, "Judiciary")

        # Initial Membership (2020-01-01): Finance and Judiciary are active
        db.execute(
            """
                SELECT upsert_legislator(
                    'Sync', 'Test', 'u12', '1', 'z', 'a',
                    '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s, %s]
                );
            """,
            (finance_id, judiciary_id),
        )

        # Second Call (2021-01-01): Only Finance remains. Judiciary is dropped.
        db.execute(
            """
                SELECT upsert_legislator(
                    'Sync', 'Test', 'u12', '1', 'z', 'a',
                    '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s]
                );
            """,
            (finance_id,),
        )

        # Assert 1: Three records in total (2 old, 1 new)
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 3

        # Assert 2: Judiciary membership must be closed
        db.execute(
            "SELECT membership_end FROM committee_membership WHERE fk_committee_id = %s AND membership_start = '2020-01-01';",
            (judiciary_id,),
        )
        jud_end_date = db.fetchone()["membership_end"]
        assert str(jud_end_date) == "2020-12-31", (
            "Judiciary membership should be closed the day before the new start date"
        )

        # Assert 3: Finance membership from 2020 must be closed (SCD-2 transition)
        db.execute(
            "SELECT membership_end FROM committee_membership WHERE fk_committee_id = %s AND membership_start = '2020-01-01';",
            (finance_id,),
        )
        fin_old_end_date = db.fetchone()["membership_end"]
        assert str(fin_old_end_date) == "2020-12-31", (
            "Finance 2020 membership should be closed to start 2021 membership"
        )

        # Assert 4: Finance membership from 2021 must be OPEN
        db.execute(
            "SELECT membership_end FROM committee_membership WHERE fk_committee_id = %s AND membership_start = '2021-01-01';",
            (finance_id,),
        )
        fin_new_end_date = db.fetchone()["membership_end"]
        assert fin_new_end_date is None, "Finance 2021 membership must be open"

    def test_new_membership_is_inserted_if_it_was_previously_closed(self, db):
        leg_id = _insert_legislator(db, "Rejoin", "Test", "u13")
        science_id = self._insert_committee(db, "Science")

        # 1. Initial Membership (2018-01-01): Science is active
        db.execute(
            """
                SELECT upsert_legislator(
                    'Rejoin', 'Test', 'u13', '1', 'z', 'a',
                    '1', 1, 'house', 'D', '2018-01-01', ARRAY[%s]
                );
            """,
            (science_id,),
        )

        # 2. Sync (2019-01-01): Science is removed (closed)
        db.execute("""
                SELECT upsert_legislator(
                    'Rejoin', 'Test', 'u13', '1', 'z', 'a',
                    '1', 1, 'house', 'D', '2019-01-01', ARRAY[]::INT[] -- Empty array removes all
                );
            """)

        # 3. Rejoin (2020-01-01): Science is added back
        db.execute(
            """
                SELECT upsert_legislator(
                    'Rejoin', 'Test', 'u13', '1', 'z', 'a',
                    '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s]
                );
            """,
            (science_id,),
        )

        # Assert 1: Three records in total for Science (Old open, closed, new open)
        db.execute(
            "SELECT COUNT(*) FROM committee_membership WHERE fk_committee_id = %s;", (science_id,),
        )
        assert db.fetchone()["count"] == 3

        # Assert 2: Check the three status transitions
        db.execute(
            """
                   SELECT membership_start, membership_end FROM committee_membership
                   WHERE fk_committee_id = %s
                   ORDER BY membership_start;
                   """,
            (science_id,),
        )
        results = db.fetchall()

        assert str(results[0]["membership_start"]) == "2018-01-01"
        assert str(results[0]["membership_end"]) == "2018-12-31"  # Closed in step 2

        assert str(results[1]["membership_start"]) == "2019-01-01"
        assert str(results[1]["membership_end"]) == "2019-12-31"  # Closed in step 3

        assert str(results[2]["membership_start"]) == "2020-01-01"
        assert results[2]["membership_end"] is None  # New open record

    # ----------------------------------------------------------------------
    # 1. Basic Insert (Happy Path)
    # ----------------------------------------------------------------------



    def test_insert_new_legislator_creates_history(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Alice', 'Jones', 'url1', '111', 'a@a', 'addr',
                '10', 5, 'house', 'D', '2020-01-01' -- Lowercase 'house'
            );
        """)

        # Legislator should exist
        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 1 # FIX

        # One history record
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1 # FIX

    # ----------------------------------------------------------------------
    # 2. Identity Resolution (Match First/Last/URL)
    # ----------------------------------------------------------------------

    def test_does_not_duplicate_legislator_when_identity_matches(self, db):
        # First insert
        db.execute("""
            SELECT upsert_legislator(
                'Bob', 'Smith', 'url2', '222', 'b@b', 'addr1',
                '7', 3, 'senate', 'R', '2019-05-01' -- Lowercase 'senate'
            );
        """)

        # Second insert with same identity (should update, not insert new)
        db.execute("""
            SELECT upsert_legislator(
                'Bob', 'Smith', 'url2', '999', 'new@b', 'addr1-new',
                '7', 3, 'senate', 'R', '2019-05-01' -- Lowercase 'senate'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 1

    def test_identity_collision_on_name_only(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Collision', 'Test', 'urlA', '1', 'x', 'a',
                '5', 1, 'house', 'D', '2020-01-01'
            );
        """)

        db.execute("""
            SELECT upsert_legislator(
                'Collision', 'Test', 'urlB', '2', 'y', 'b',
                '5', 1, 'house', 'D', '2020-01-01'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 2, "Expect two distinct legislators"

    # ----------------------------------------------------------------------
    # 3. SCD-1 Updates (Only updates phone/email/address)
    # ----------------------------------------------------------------------

    def test_scd1_updates_only_non_history_fields(self, db):
        # Insert baseline state
        db.execute("""
            SELECT upsert_legislator(
                'Carl', 'Lee', 'u3', '333', 'c@c', 'a1',
                '12', 1, 'house', 'I', '2018-01-01' -- Lowercase 'house'
            );
        """)

        # Update only SCD1 fields → should NOT create new history row
        db.execute("""
            SELECT upsert_legislator(
                'Carl', 'Lee', 'u3', '444', 'updated@c', 'a2',
                '12', 1, 'house', 'I', '2018-01-01' -- Lowercase 'house'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    def test_scd1_update_from_null_to_value(self, db):
        # Initial insert with NULL phone and email
        db.execute("""
            SELECT upsert_legislator(
                'Scd1', 'NullA', 'urlA', NULL, NULL, 'addr',
                '1', 1, 'house', 'D', '2020-01-01'
            );
        """)

        # Update with actual values (SCD1 change)
        db.execute("""
            SELECT upsert_legislator(
                'Scd1', 'NullA', 'urlA', '555-5555', 'z@z.com', 'addr',
                '1', 1, 'house', 'D', '2020-01-01'
            );
        """)

        # Assert SCD1 fields updated in the main table
        db.execute("SELECT phone, email FROM legislators WHERE url = 'urlA';")
        result = db.fetchone()
        assert result["phone"] == "555-5555"
        assert result["email"] == "z@z.com"

        # Assert SCD2 was NOT triggered
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    def test_scd1_update_from_value_to_null(self, db):
        # Initial insert with values
        db.execute("""
            SELECT upsert_legislator(
                'Scd1', 'NullB', 'urlB', '555-5555', 'z@z.com', 'addr',
                '1', 1, 'house', 'D', '2020-01-01'
            );
        """)

        # Update by passing NULL for phone/email (SCD1 change)
        db.execute("""
            SELECT upsert_legislator(
                'Scd1', 'NullB', 'urlB', NULL, NULL, 'addr',
                '1', 1, 'house', 'D', '2020-01-01'
            );
        """)

        # Assert SCD1 fields updated to NULL in the main table
        db.execute("SELECT phone, email FROM legislators WHERE url = 'urlB';")
        result = db.fetchone()
        assert result["phone"] is None
        assert result["email"] is None

        # Assert SCD2 was NOT triggered
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 4. SCD-2: Changing district/party/chamber
    # ----------------------------------------------------------------------

    def test_scd2_creates_new_history_when_district_changes(self, db):
        # Initial record
        db.execute("""
            SELECT upsert_legislator(
                'Dana', 'Ray', 'u4', '444', 'd@d', 'addr',
                '1', 2, 'house', 'D', '2020-01-01' -- Lowercase 'house'
            );
        """)

        # Change district (SCD2 trigger)
        db.execute("""
            SELECT upsert_legislator(
                'Dana', 'Ray', 'u4', '444', 'd@d', 'addr',
                '2', 2, 'house', 'D', '2021-01-01' -- Lowercase 'house'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2 # FIX

        # Check end_date of first record
        db.execute("""
                   SELECT end_date FROM legislator_history
                   WHERE end_date IS NOT NULL;
                   """)
        end_date = db.fetchone()["end_date"]
        assert str(end_date) == "2020-12-31"

    def test_scd2_triggers_on_party_change(self, db):
        # Initial
        db.execute("""
            SELECT upsert_legislator(
                'Eve', 'Adams', 'u5', '555', 'e@e', 'addr',
                '3', 1, 'house', 'D', '2020-01-01' -- Lowercase 'house'
            );
        """)

        # Party change
        db.execute("""
            SELECT upsert_legislator(
                'Eve', 'Adams', 'u5', '555', 'e@e', 'addr',
                '3', 1, 'house', 'R', '2021-01-01' -- Lowercase 'house'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    # ----------------------------------------------------------------------
    # 5. No-Change: SCD-2 does NOT run if chamber/party/district identical
    # ----------------------------------------------------------------------

    def test_no_new_history_if_nothing_changes(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Frank', 'Hall', 'u6', '111', 'f@f', 'addr',
                '5', 1, 'senate', 'I', '2020-01-01' -- Lowercase 'senate'
            );
        """)

        # Try again with identical values
        db.execute("""
            SELECT upsert_legislator(
                'Frank', 'Hall', 'u6', '111', 'f@f', 'addr',
                '5', 1, 'senate', 'I', '2020-01-01' -- Lowercase 'senate'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 6. Edge Case: Changing chamber
    # ----------------------------------------------------------------------

    def test_scd2_triggers_on_chamber_change(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Gail', 'Ivy', 'u7', '222', 'g@g', 'addr',
                '6', 2, 'house', 'D', '2018-01-01' -- Lowercase 'house'
            );
        """)

        db.execute("""
            SELECT upsert_legislator(
                'Gail', 'Ivy', 'u7', '222', 'g@g', 'addr',
                '6', 2, 'senate', 'D', '2020-01-01' -- Lowercase 'senate'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    # ----------------------------------------------------------------------
    # 7. Start-Date Edge Case (backdated update)
    # ----------------------------------------------------------------------

    def test_history_end_date_uses_start_date_minus_one(self, db):
        # FIRST CALL: Earliest date
        db.execute("""
            SELECT upsert_legislator(
                'Harry', 'Young', 'u8', '333', 'h@h', 'addr',
                '1', 10, 'house', 'D', '2022-01-05' -- Earliest date
            );
        """)

        # SECOND CALL: Later date (Change)
        db.execute("""
            SELECT upsert_legislator(
                'Harry', 'Young', 'u8', '333', 'h@h', 'addr',
                '2', 10, 'house', 'D', '2022-01-10' -- New start date
            );
        """)

        db.execute("SELECT end_date FROM legislator_history WHERE end_date IS NOT NULL;")
        end_date = db.fetchone()["end_date"]

        # Should close on the day before the *new* start date (Jan 10)
        assert str(end_date) == "2022-01-09"

    # ----------------------------------------------------------------------
    # 8. Identity Collision: Same name different URL = new legislator
    # ----------------------------------------------------------------------

    def test_same_name_different_url_creates_new_legislator(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Ivan', 'Stone', 'urlA', '1', 'x', 'a',
                '5', 1, 'house', 'D', '2020-01-01' -- Lowercase 'house'
            );
        """)

        db.execute("""
            SELECT upsert_legislator(
                'Ivan', 'Stone', 'urlB', '1', 'x', 'a',
                '5', 1, 'house', 'D', '2020-01-01' -- Lowercase 'house'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 2

    # ----------------------------------------------------------------------
    # 9. Multiple successive SCD2 changes
    # ----------------------------------------------------------------------

    def test_multiple_scd2_changes_create_multiple_history_rows(self, db):
        # Insert original
        db.execute("""
            SELECT upsert_legislator(
                'Jane', 'King', 'u9', '444', 'j@j', 'addr',
                '1', 1, 'house', 'D', '2020-01-01' -- Lowercase 'house'
            );
        """)

        # Change 1 (district)
        db.execute("""
            SELECT upsert_legislator(
                'Jane', 'King', 'u9', '444', 'j@j', 'addr',
                '2', 1, 'house', 'D', '2021-01-01' -- Lowercase 'house'
            );
        """)

        # Change 2 (party)
        db.execute("""
            SELECT upsert_legislator(
                'Jane', 'King', 'u9', '444', 'j@j', 'addr',
                '2', 1, 'house', 'R', '2022-01-01' -- Lowercase 'house'
            );
        """)

        # Change 3 (chamber)
        db.execute("""
            SELECT upsert_legislator(
                'Jane', 'King', 'u9', '444', 'j@j', 'addr',
                '2', 1, 'senate', 'R', '2023-01-01' -- Lowercase 'senate'
            );
        """)

        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 4  # FIX

    def test_scd2_trigger_on_district_from_null(self, db):
        # Initial insert with NULL district (SCD2 field)
        db.execute("""
            SELECT upsert_legislator(
                'Scd2', 'NullC', 'urlC', '1', 'x', 'a',
                NULL, 1, 'house', 'D', '2020-01-01'
            );
        """)

        # Update with a valid district value (NULL IS DISTINCT FROM '1')
        db.execute("""
            SELECT upsert_legislator(
                'Scd2', 'NullC', 'urlC', '1', 'x', 'a',
                '1', 1, 'house', 'D', '2021-01-01'
            );
        """)

        # Assert SCD2 was triggered
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_trigger_on_party_to_null(self, db):
        # Initial insert with a valid party
        db.execute("""
            SELECT upsert_legislator(
                'Scd2', 'NullD', 'urlD', '1', 'x', 'a',
                '1', 1, 'house', 'D', '2020-01-01'
            );
        """)

        # Update by passing NULL for party (Value 'D' IS DISTINCT FROM NULL)
        db.execute("""
            SELECT upsert_legislator(
                'Scd2', 'NullD', 'urlD', '1', 'x', 'a',
                '1', 1, 'house', NULL, '2021-01-01'
            );
        """)

        # Assert SCD2 was triggered
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_no_trigger_on_seniority_change(self, db):
        # Initial record
        db.execute("""
            SELECT upsert_legislator(
                'No', 'Trigger', 'uX', '111', 'x@x', 'a1',
                '12', 1, 'house', 'I', '2018-01-01'
            );
        """)

        # Update only seniority (NOT an SCD2 trigger field in your function)
        db.execute("""
            SELECT upsert_legislator(
                'No', 'Trigger', 'uX', '111', 'x@x', 'a1',
                '12', 5, 'house', 'I', '2018-01-01'
            );
        """)

        # Assert SCD2 was NOT triggered
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

