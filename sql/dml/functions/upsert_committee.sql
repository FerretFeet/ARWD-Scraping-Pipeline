CREATE OR REPLACE FUNCTION upsert_committee(
    p_committee_id INT,
    p_name TEXT,
    p_url TEXT,
    p_session_code VARCHAR(20)
)
RETURNS INT AS $$
DECLARE
    v_start_date DATE;
    v_open RECORD;
    v_has_any BOOLEAN;
BEGIN
    -- Resolve session start date
    SELECT start_date
    INTO v_start_date
    FROM sessions
    WHERE session_code = p_session_code;

    IF v_start_date IS NULL THEN
        RAISE EXCEPTION 'Session code % not found.', p_session_code;
    END IF;

    -- Ensure committee exists
    INSERT INTO committees (committee_id)
    VALUES (p_committee_id)
    ON CONFLICT DO NOTHING;

    -- Does ANY record exist?
    SELECT EXISTS (
        SELECT 1
        FROM committee_info
        WHERE fk_committee_id = p_committee_id
          AND committee_name = p_name
    )
    INTO v_has_any;

    -- Fetch OPEN record, if one exists
    SELECT *
    INTO v_open
    FROM committee_info
    WHERE fk_committee_id = p_committee_id
      AND committee_name = p_name
      AND end_date IS NULL
    LIMIT 1;

    ----------------------------------------------------------------
    -- CASE 1: open record exists and data is identical → NO-OP
    ----------------------------------------------------------------
    IF v_open.committee_info_id IS NOT NULL
       AND v_open.url IS NOT DISTINCT FROM p_url
    THEN
        RETURN p_committee_id;
    END IF;

    ----------------------------------------------------------------
    -- CASE 2: open record exists and data changed → CLOSE + INSERT
    ----------------------------------------------------------------
    IF v_open.committee_info_id IS NOT NULL THEN
        UPDATE committee_info
        SET end_date = GREATEST(v_start_date - INTERVAL '1 day', v_open.start_date)
        WHERE committee_info_id = v_open.committee_info_id;
    END IF;

    ----------------------------------------------------------------
    -- CASE 3: no open record OR data changed OR no records at all
    ----------------------------------------------------------------
    INSERT INTO committee_info (
        fk_committee_id,
        committee_name,
        url,
        start_date,
        end_date
    )
    VALUES (
        p_committee_id,
        p_name,
        p_url,
        v_start_date,
        NULL
    );

    RETURN p_committee_id;
END;
$$ LANGUAGE plpgsql;
