import psycopg
import pytest


@pytest.fixture
def sql_file():
    return "dml/upsert_legislator.sql"

# --------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------

def _insert_legislator(db, first, last, phone=None, email=None, address=None):
    db.execute("""
        INSERT INTO legislators (first_name, last_name, phone, email, address)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING legislator_id
    """, (first, last, phone, email, address))
    return db.fetchone()["legislator_id"]

def _insert_legislator_history(db, legislator_id, district, seniority, chamber, party,
                               start_date, url="placeholder", end_date=None):
    db.execute("""
        INSERT INTO legislator_history
            (fk_legislator_id, district, seniority, chamber, url, party, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (legislator_id, district, seniority, chamber, url, party, start_date, end_date))

def _insert_committee(db, name, url=None):
    db.execute("""
        INSERT INTO committees (name, url)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE
        SET url = COALESCE(EXCLUDED.url, committees.url)
        RETURNING committee_id
    """, (name, url))
    return db.fetchone()["committee_id"]

# ===========================================================================
# Test Suite
# ===========================================================================

class TestUpsertLegislator:

    # ----------------------------------------------------------------------
    # 1. Basic insert
    # ----------------------------------------------------------------------
    def test_insert_new_legislator_creates_history(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Alice', 'Jones', 'url1', '111', 'a@a', 'addr',
                '10', 5, 'house', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 1
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 2. Identity resolution
    # ----------------------------------------------------------------------
    def test_does_not_duplicate_legislator_when_identity_matches(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Bob', 'Smith', 'url2', '222', 'b@b', 'addr1',
                '7', 3, 'senate', 'R', '2019-05-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'Bob', 'Smith', 'url2', '999', 'new@b', 'addr1-new',
                '7', 3, 'senate', 'R', '2019-05-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislators;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 3. SCD1 updates
    # ----------------------------------------------------------------------
    def test_scd1_updates_only_non_history_fields(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Carl', 'Lee', 'u3', '333', 'c@c', 'a1',
                '12', 1, 'house', 'I', '2018-01-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'Carl', 'Lee', 'u3', '444', 'updated@c', 'a2',
                '12', 1, 'house', 'I', '2018-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 4. SCD2: changing district/party/chamber/url
    # ----------------------------------------------------------------------
    def test_scd2_creates_new_history_when_district_changes(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Dana', 'Ray', 'u4', '444', 'd@d', 'addr',
                '1', 2, 'house', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'Dana', 'Ray', 'u4', '444', 'd@d', 'addr',
                '2', 2, 'house', 'D', '2021-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_triggers_on_party_change(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Eve', 'Adams', 'u5', '555', 'e@e', 'addr',
                '3', 1, 'house', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'Eve', 'Adams', 'u5', '555', 'e@e', 'addr',
                '3', 1, 'house', 'R', '2021-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_scd2_triggers_on_chamber_change(self, db):
        db.execute("""
            SELECT upsert_legislator(
                'Gail', 'Ivy', 'u7', '222', 'g@g', 'addr',
                '6', 2, 'house', 'D', '2018-01-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'Gail', 'Ivy', 'u7', '222', 'g@g', 'addr',
                '6', 2, 'senate', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    # ----------------------------------------------------------------------
    # 5. Committee memberships
    # ----------------------------------------------------------------------
    def test_insert_new_committees_on_first_upsert(self, db):
        ag_id = _insert_committee(db, "Agriculture")
        budget_id = _insert_committee(db, "Budget")
        db.execute("""
            SELECT upsert_legislator(
                'Katie', 'Lee', 'u10', '1', 'k@k', 'a',
                '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s, %s]
            );
        """, (ag_id, budget_id))
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 2

    def test_re_running_upsert_does_not_duplicate_memberships(self, db):
        ag_id = _insert_committee(db, "Agriculture-B")
        db.execute("""
            SELECT upsert_legislator(
                'Sam', 'Wise', 'u11', '1', 's@s', 'a',
                '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s]
            );
        """, (ag_id,))
        db.execute("""
            SELECT upsert_legislator(
                'Sam', 'Wise', 'u11', '1', 's@s', 'a',
                '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s]
            );
        """, (ag_id,))
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_existing_memberships_are_closed_if_removed_from_list(self, db):
        leg_id = _insert_legislator(db, "Sync", "Test")
        finance_id = _insert_committee(db, "Finance")
        judiciary_id = _insert_committee(db, "Judiciary")

        # Initial insert with both committees
        db.execute("""
            SELECT upsert_legislator(
                'Sync', 'Test', 'u12', '1', 'z', 'a',
                '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s, %s]
            );
        """, (finance_id, judiciary_id))

        # Second insert with only finance
        db.execute("""
            SELECT upsert_legislator(
                'Sync', 'Test', 'u12', '1', 'z', 'a',
                '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s]
            );
        """, (finance_id,))

        # Verify Judiciary membership is closed
        db.execute("""
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2020-01-01';
                   """, (judiciary_id,))
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

        # Verify Finance old membership is still open (continuous)
        db.execute("""
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2020-01-01';
                   """, (finance_id,))
        assert db.fetchone()["membership_end"] is None

        # Verify Finance new membership (if inserted) is open
        db.execute("""
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2021-01-01';
                   """, (finance_id,))
        row = db.fetchone()
        if row:
            assert row["membership_end"] is None

        # ----------------------------------------------------------------------
        # 6. Committee membership edge cases
        # ----------------------------------------------------------------------

    def test_committee_membership_array_null(self, db):
        """Handles NULL committee array: no memberships should be created, existing closed."""
        leg_id = _insert_legislator(db, "Null", "Committee")
        finance_id = _insert_committee(db, "Finance")

        # Insert with a committee first
        db.execute(
            """
               SELECT upsert_legislator(
                   'Null', 'Committee', 'u20', '1', 'n@n', 'addr',
                   '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s]
               );
           """,
            (finance_id,),
        )

        # Second insert with NULL array: should close Finance membership
        db.execute("""
               SELECT upsert_legislator(
                   'Null', 'Committee', 'u20', '1', 'n@n', 'addr',
                   '1', 1, 'house', 'D', '2021-01-01', NULL
               );
           """)
        db.execute(
            """
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2020-01-01';
                   """,
            (finance_id,),
        )
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

    def test_committee_membership_array_empty(self, db):
        """Handles empty array: same behavior as NULL (close all existing)."""
        leg_id = _insert_legislator(db, "Empty", "Committee")
        finance_id = _insert_committee(db, "Finance-E")

        db.execute(
            """
               SELECT upsert_legislator(
                   'Empty', 'Committee', 'u21', '1', 'e@e', 'addr',
                   '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s]
               );
           """,
            (finance_id,),
        )

        db.execute("""
               SELECT upsert_legislator(
                   'Empty', 'Committee', 'u21', '1', 'e@e', 'addr',
                   '1', 1, 'house', 'D', '2021-01-01', ARRAY[]::int[]
               );
           """)
        db.execute(
            """
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2020-01-01';
                   """,
            (finance_id,),
        )
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

    def test_multiple_committee_memberships_added_and_removed(self, db):
        """Add multiple committees, remove one, verify only removed is closed."""
        leg_id = _insert_legislator(db, "Multi", "Committee")
        c1 = _insert_committee(db, "C1")
        c2 = _insert_committee(db, "C2")
        c3 = _insert_committee(db, "C3")

        # First insert with three committees
        db.execute(
            """
               SELECT upsert_legislator(
                   'Multi', 'Committee', 'u22', '1', 'm@m', 'addr',
                   '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s, %s, %s]
               );
           """,
            (c1, c2, c3),
        )

        # Second insert with two committees (remove C2)
        db.execute(
            """
               SELECT upsert_legislator(
                   'Multi', 'Committee', 'u22', '1', 'm@m', 'addr',
                   '1', 1, 'house', 'D', '2021-01-01', ARRAY[%s, %s]
               );
           """,
            (c1, c3),
        )

        # Verify C2 closed
        db.execute(
            """
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2020-01-01';
                   """,
            (c2,),
        )
        assert str(db.fetchone()["membership_end"]) == "2020-12-31"

        # Verify C1 and C3 still open
        for cid in (c1, c3):
            db.execute(
                """
                       SELECT membership_end FROM committee_membership
                       WHERE fk_committee_id = %s AND membership_start = '2020-01-01';
                       """,
                (cid,),
            )
            assert db.fetchone()["membership_end"] is None

    def test_committee_membership_duplicate_dates(self, db):
        """Re-running upsert with same start_date does not duplicate memberships."""
        leg_id = _insert_legislator(db, "Dup", "Committee")
        cid = _insert_committee(db, "DupC")

        db.execute(
            """
               SELECT upsert_legislator(
                   'Dup', 'Committee', 'u23', '1', 'd@d', 'addr',
                   '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s]
               );
           """,
            (cid,),
        )
        db.execute(
            """
               SELECT upsert_legislator(
                   'Dup', 'Committee', 'u23', '1', 'd@d', 'addr',
                   '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s]
               );
           """,
            (cid,),
        )
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_committee_duplicates_in_array(self, db):
        leg_id = _insert_legislator(db, "DupArray", "Test")
        c1 = _insert_committee(db, "C-Dup")
        # Duplicate IDs in array
        db.execute("""
            SELECT upsert_legislator(
                'DupArray', 'Test', 'u30', '1', 'd@d', 'addr',
                '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s, %s, %s]
            );
        """, (c1, c1, c1))
        db.execute("SELECT COUNT(*) FROM committee_membership;")
        assert db.fetchone()["count"] == 1

    def test_committee_nonexistent_ids(self, db):
        leg_id = _insert_legislator(db, "Nonexistent", "Test")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            db.execute("""
                SELECT upsert_legislator(
                    'Nonexistent', 'Test', 'u31', '1', 'n@n', 'addr',
                    '1', 1, 'house', 'D', '2020-01-01', ARRAY[99999]
                );
            """)  # Should fail

    # ----------------------------------------------------------------------
    # 8. SCD2 comprehensive triggers
    # ----------------------------------------------------------------------
    def test_history_triggers_on_url_change(self, db):
        leg_id = _insert_legislator(db, "URL", "Test")
        db.execute("""
            SELECT upsert_legislator(
                'URL', 'Test', 'old_url', '1', 'u@u', 'addr',
                '1', 1, 'house', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'URL', 'Test', 'new_url', '1', 'u@u', 'addr',
                '1', 1, 'house', 'D', '2021-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

    def test_history_no_duplicate_on_same_data(self, db):
        leg_id = _insert_legislator(db, "Same", "Test")
        db.execute("""
            SELECT upsert_legislator(
                'Same', 'Test', 'url', '1', 's@s', 'addr',
                '1', 1, 'house', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("""
            SELECT upsert_legislator(
                'Same', 'Test', 'url', '1', 's@s', 'addr',
                '1', 1, 'house', 'D', '2020-01-01', NULL
            );
        """)
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 1

    # ----------------------------------------------------------------------
    # 9. Combination edge cases
    # ----------------------------------------------------------------------
    def test_history_and_committee_change_simultaneously(self, db):
        leg_id = _insert_legislator(db, "Combo", "Test")
        c1 = _insert_committee(db, "Combo1")
        c2 = _insert_committee(db, "Combo2")

        # Initial upsert
        db.execute("""
            SELECT upsert_legislator(
                'Combo', 'Test', 'url1', '1', 'c@c', 'addr',
                '1', 1, 'house', 'D', '2020-01-01', ARRAY[%s]
            );
        """, (c1,))

        # Second upsert: change party and committee membership
        db.execute("""
            SELECT upsert_legislator(
                'Combo', 'Test', 'url1', '1', 'c@c', 'addr',
                '1', 1, 'house', 'R', '2021-01-01', ARRAY[%s, %s]
            );
        """, (c1, c2))

        # History count
        db.execute("SELECT COUNT(*) FROM legislator_history;")
        assert db.fetchone()["count"] == 2

        # Committee membership check
        db.execute("""
                   SELECT membership_end FROM committee_membership
                   WHERE fk_committee_id = %s AND membership_start = '2021-01-01';
                   """, (c2,))
        assert db.fetchone()["membership_end"] is None
