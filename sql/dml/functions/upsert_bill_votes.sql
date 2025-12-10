CREATE OR REPLACE FUNCTION upsert_bill_votes(
    p_bill_id BIGINT,
    p_vote_timestamp TIMESTAMPTZ,
    p_chamber chamber,
    p_motion_text TEXT,
    p_yea_voters JSONB,
    p_nay_voters JSONB,
    p_non_voting_voters JSONB,
    p_present_voters JSONB,
    p_excused_voters JSONB
) RETURNS BIGINT AS $$
DECLARE
    v_vote_event_id BIGINT;
    v_leg_id BIGINT;
    v_bucket TEXT;
    v_elem TEXT;
BEGIN
    -- 1) Upsert vote_event
    INSERT INTO vote_events(fk_bill_id, vote_timestamp, chamber, motion_text)
    VALUES (p_bill_id, p_vote_timestamp, p_chamber, p_motion_text)
    ON CONFLICT (fk_bill_id, chamber, vote_timestamp)
    DO UPDATE SET motion_text = EXCLUDED.motion_text
    RETURNING vote_event_id INTO v_vote_event_id;

    -- 2) Delete existing votes for this event (idempotency)
    DELETE FROM legislator_votes
    WHERE fk_vote_event_id = v_vote_event_id;

    -- 3) Insert votes from JSONB arrays wrapped in objects
    FOR v_bucket, v_elem IN
        SELECT 'yea_voters', jsonb_array_elements_text(p_yea_voters -> 'yea_voters') UNION ALL
        SELECT 'nay_voters', jsonb_array_elements_text(p_nay_voters -> 'nay_voters') UNION ALL
        SELECT 'non_voting_voters', jsonb_array_elements_text(p_non_voting_voters -> 'non_voting_voters') UNION ALL
        SELECT 'present_voters', jsonb_array_elements_text(p_present_voters -> 'present_voters') UNION ALL
        SELECT 'excused_voters', jsonb_array_elements_text(p_excused_voters -> 'excused_voters')
    LOOP
        v_leg_id := NULLIF(trim(v_elem), '')::BIGINT;
        IF v_leg_id IS NOT NULL THEN
            INSERT INTO legislator_votes(fk_vote_event_id, fk_legislator_id, vote_cast)
            VALUES (
                v_vote_event_id,
                v_leg_id,
                CASE v_bucket
                    WHEN 'yea_voters' THEN 'yea'::vote_type
                    WHEN 'nay_voters' THEN 'nay'::vote_type
                    WHEN 'non_voting_voters' THEN 'non_voting'::vote_type
                    WHEN 'present_voters' THEN 'present'::vote_type
                    WHEN 'excused_voters' THEN 'excused'::vote_type
                END
            );
        END IF;
    END LOOP;

    RETURN v_vote_event_id;
END;
$$ LANGUAGE plpgsql;
