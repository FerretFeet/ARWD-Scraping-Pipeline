import datetime
from pathlib import Path

import psycopg
import pytest

from src.data_pipeline.load.pipeline_loader import PipelineLoader


@pytest.fixture
def sql_file():
    return "dml/upsert_bill_votes.sql"

@pytest.fixture
def sql_file_path() -> Path:
    """Return the path to the upsert_bill_votes SQL file."""
    return Path("sql/dml/upsert_bill_votes.sql")


@pytest.fixture(autouse=True)
def clean_tables(db):
    db.execute("""
        TRUNCATE legislator_votes, vote_events, bills, sessions
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
    row = db.fetchone()
    return row["session_code"] if row else "2025A"


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
def setup_bill(db, setup_session):
    db.execute("""
        INSERT INTO bills (bill_no, title, url, fk_session_code, intro_date)
        VALUES ('B-001', 'Test Bill', 'http://example.com/bill', %s, '2025-01-01')
        RETURNING bill_id;
    """, (setup_session,))
    return db.fetchone()["bill_id"]


@pytest.fixture
def sample_vote_data(setup_legislators, setup_bill):
    # Wrap lists in dicts to match updated function input
    return {
        "bill_id": setup_bill,
        "vote_timestamp": datetime.datetime(2025, 3, 1, 12, 0, tzinfo=datetime.UTC),
        "chamber": "house",
        "motion_text": "Motion to pass",
        "yea_voters": {"yea_voters": [int(setup_legislators[0]), int(setup_legislators[1])]},
        "nay_voters": {"nay_voters": [int(setup_legislators[2])]},
        "non_voting_voters": {"non_voting_voters": []},
        "present_voters": {"present_voters": []},
        "excused_voters": {"excused_voters": []},
    }


@pytest.fixture
def loader(sql_file_path):
    return PipelineLoader(
        sql_file_path=sql_file_path,
        upsert_function_name="upsert_bill_votes",
        required_params={
            "bill_id": int,
            "vote_timestamp": datetime.datetime,
            "chamber": str,
            "motion_text": str,
            "yea_voters": dict,
            "nay_voters": dict,
            "non_voting_voters": dict,
            "present_voters": dict,
            "excused_voters": dict,
        },
        insert="""
            SELECT upsert_bill_votes(
                p_bill_id := %(p_bill_id)s,
                p_vote_timestamp := %(p_vote_timestamp)s,
                p_chamber := %(p_chamber)s,
                p_motion_text := %(p_motion_text)s,
                p_yea_voters := %(p_yea_voters)s::JSONB,
                p_nay_voters := %(p_nay_voters)s::JSONB,
                p_non_voting_voters := %(p_non_voting_voters)s::JSONB,
                p_present_voters := %(p_present_voters)s::JSONB,
                p_excused_voters := %(p_excused_voters)s::JSONB
            );
        """,
    )


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

class TestPipelineLoaderVoteEvent:
    def test_insert_all_vote_types(self, db, loader, sample_vote_data):
        vote_row = loader.execute(sample_vote_data, db)
        vote_id = list(vote_row.values())[0]  # get the returned BIGINT
        assert vote_id is not None

        db.execute("SELECT COUNT(*) FROM legislator_votes WHERE fk_vote_event_id = %s", (vote_id,))
        assert db.fetchone()["count"] == 3  # 2 yea + 1 nay

    def test_idempotent_double_insert(self, db, loader, sample_vote_data):
        first_row = loader.execute(sample_vote_data, db)
        first_id = list(first_row.values())[0]
        second_row = loader.execute(sample_vote_data, db)
        second_id = list(second_row.values())[0]
        assert first_id == second_id

        db.execute("SELECT COUNT(*) FROM vote_events;")
        assert db.fetchone()["count"] == 1
        db.execute("SELECT COUNT(*) FROM legislator_votes;")
        assert db.fetchone()["count"] == 3


    def test_update_motion_text(self, db, loader, sample_vote_data):
        loader.execute(sample_vote_data, db)
        sample_vote_data["motion_text"] = "Updated Motion"
        loader.execute(sample_vote_data, db)

        db.execute("SELECT motion_text FROM vote_events WHERE fk_bill_id = %s", (sample_vote_data["bill_id"],))
        assert db.fetchone()["motion_text"] == "Updated Motion"

    def test_empty_vote_lists(self, db, loader, sample_vote_data):
        for key in ["yea_voters", "nay_voters", "non_voting_voters", "present_voters", "excused_voters"]:
            sample_vote_data[key] = {key: []}
        loader.execute(sample_vote_data, db)

        db.execute("SELECT COUNT(*) FROM legislator_votes;")
        assert db.fetchone()["count"] == 0

    def test_partial_votes(self, db, loader, sample_vote_data):
        for key in ["nay_voters", "non_voting_voters", "present_voters", "excused_voters"]:
            sample_vote_data[key] = {key: []}
        loader.execute(sample_vote_data, db)

        db.execute("SELECT COUNT(*) FROM legislator_votes;")
        assert db.fetchone()["count"] == 2  # only 2 yea votes

    def test_invalid_vote_timestamp(self, db, loader, sample_vote_data):
        sample_vote_data["vote_timestamp"] = "not-a-date"
        import pytest
        with pytest.raises(psycopg.errors.InvalidDatetimeFormat):
            loader.execute(sample_vote_data, db)
