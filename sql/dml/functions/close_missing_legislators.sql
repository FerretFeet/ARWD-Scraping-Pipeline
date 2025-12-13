CREATE OR REPLACE FUNCTION close_missing_legislators(
    p_active_urls TEXT[],
    p_session_code VARCHAR
) RETURNS VOID AS $$
DECLARE
    v_start_date DATE;
BEGIN
    SELECT start_date INTO v_start_date
    FROM sessions
    WHERE session_code = p_session_code
    LIMIT 1;

    IF v_start_date IS NULL THEN
        RAISE EXCEPTION 'No session found for session_code: %', p_session_code;
    END IF;

    /*
     * Identify legislators who were active going into this session
     * but do not appear in the current scrape
     */
    WITH to_close AS (
        SELECT lh.history_id, lh.fk_legislator_id
        FROM legislator_history lh
        WHERE lh.end_date IS NULL
          AND lh.start_date < v_start_date
          AND NOT (lh.url = ANY (COALESCE(p_active_urls, ARRAY[]::text[])))
    )
    -- 1. Close legislator history
    UPDATE legislator_history lh
    SET end_date = v_start_date - INTERVAL '1 day'
    FROM to_close tc
    WHERE lh.history_id = tc.history_id;

     WITH to_close AS (
        SELECT lh.history_id, lh.fk_legislator_id
        FROM legislator_history lh
        WHERE lh.end_date IS NULL
          AND lh.start_date < v_start_date
          AND NOT (lh.url = ANY (COALESCE(p_active_urls, ARRAY[]::text[])))
    )
    -- 2. Close committee memberships for those legislators
    UPDATE committee_membership cm
    SET membership_end = v_start_date - INTERVAL '1 day'
    WHERE cm.membership_end IS NULL
      AND cm.membership_start < v_start_date
      AND cm.fk_legislator_id IN (
          SELECT fk_legislator_id FROM to_close
      );

END;
$$ LANGUAGE plpgsql;
