CREATE OR REPLACE FUNCTION upsert_bill_with_sponsors(
    p_title TEXT,
    p_bill_no VARCHAR,
    p_url TEXT,
    p_session_code VARCHAR,
    p_intro_date TIMESTAMP,
    p_act_date DATE,
    p_bill_documents JSONB,          -- {"amendments":[...], "bill_text":[...], "act_text":[...]}
    p_lead_sponsor JSONB,            -- {"committee_id":[1]} OR {"legislator_id":[...]}
    p_other_primary_sponsor JSONB,   -- {"legislator_id":[...], "committee_id":[...]}
    p_cosponsors JSONB,               -- {"legislator_id":[...]} OR {"committee_id":[...]}
    p_bill_status_history JSONB[]            -- [{"chamber":"", "history_action":"","status_date":DatetimeObj, "vote_action_present":"T/F"}]
) RETURNS INT AS $$
DECLARE
    v_bill_id INT;
    v_existing_bill_id INT;
    v_doc_type TEXT;
    v_doc_url TEXT;
    v_leg_id INT;
    v_comm_id INT;
  v_status JSONB;
BEGIN
    -- 1) Find existing bill
    SELECT bill_id INTO v_existing_bill_id
    FROM bills
    WHERE bill_no = p_bill_no
      AND fk_session_code = p_session_code;

    IF v_existing_bill_id IS NOT NULL THEN
        v_bill_id := v_existing_bill_id;
        UPDATE bills
        SET title = p_title,
            url = p_url,
            intro_date = p_intro_date,
            act_date = p_act_date
        WHERE bill_id = v_bill_id;
    ELSE
        INSERT INTO bills (bill_no, title, url, fk_session_code, intro_date, act_date)
        VALUES (p_bill_no, p_title, p_url, p_session_code, p_intro_date, p_act_date)
        RETURNING bill_id INTO v_bill_id;
    END IF;

    -- 2) Insert bill documents
    FOR v_doc_type, v_doc_url IN
        SELECT key, jsonb_array_elements_text(value)
        FROM jsonb_each(p_bill_documents)
    LOOP
        IF coalesce(trim(v_doc_url), '') = '' THEN
            CONTINUE;
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM bill_documents
            WHERE fk_bill_id = v_bill_id
              AND document_type = v_doc_type
              AND url = v_doc_url
        ) THEN
            INSERT INTO bill_documents(fk_bill_id, document_type, url)
            VALUES (v_bill_id, v_doc_type, v_doc_url);
        END IF;
    END LOOP;

    -- 3) Insert bill status history
    IF p_bill_status_history IS NOT NULL THEN
        FOREACH v_status IN ARRAY p_bill_status_history LOOP
            -- Extract fields from JSONB object
            INSERT INTO bill_status_history(
                fk_bill_id,
                chamber,
                status_date,
                history_action,
                vote_action_present,
                vote_action
            )
            SELECT
                v_bill_id,
                v_status->>'chamber'::chamber,
                (v_status->>'status_date')::DATE,
                v_status->>'history_action',
                (v_status->>'vote_action_present')::BOOLEAN,
                NULLIF(v_status->>'vote_action','')::INT
            WHERE NOT EXISTS (
                SELECT 1
                FROM bill_status_history
                WHERE fk_bill_id = v_bill_id
                  AND chamber = v_status->>'chamber'::chamber
                  AND status_date = (v_status->>'status_date')::DATE
                  AND history_action = v_status->>'history_action'
            );
        END LOOP;
    END IF;


    -- Helper function for inserting sponsors
    -- Type TEXT: 'lead', 'primary', 'cosponsor'
    -- Key: 'legislator_id' or 'committee_id'
    -- JSONB array: the ids
    PERFORM upsert_sponsor_set(p_bill_id := v_bill_id, sponsor_type := 'lead_sponsor', sponsor_json := p_lead_sponsor);
    PERFORM upsert_sponsor_set(p_bill_id := v_bill_id, sponsor_type := 'other_primary_sponsor', sponsor_json := p_other_primary_sponsor);
    PERFORM upsert_sponsor_set(p_bill_id := v_bill_id, sponsor_type := 'cosponsor', sponsor_json := p_cosponsors);

    RETURN v_bill_id;
END;
$$ LANGUAGE plpgsql;

-- Auxiliary function for sponsor insertion
CREATE OR REPLACE FUNCTION upsert_sponsor_set(
    p_bill_id INT,
    sponsor_type TEXT,
    sponsor_json JSONB
) RETURNS VOID AS $$
DECLARE
    v_elem TEXT;
    v_id INT;
BEGIN
    IF sponsor_json IS NULL THEN
        RETURN;
    END IF;

    -- Legislators
    IF sponsor_json ? 'legislator_id' THEN
        FOR v_elem IN SELECT jsonb_array_elements_text(sponsor_json->'legislator_id') LOOP
            v_id := NULLIF(trim(v_elem), '')::INT;
            IF v_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM sponsors
                WHERE fk_bill_id = p_bill_id
                  AND fk_legislator_id = v_id
            ) THEN
                INSERT INTO sponsors(sponsor_type, fk_legislator_id, fk_bill_id)
                VALUES (sponsor_type::sponsor_type, v_id, p_bill_id);
            END IF;
        END LOOP;
    END IF;

    -- Committees
    IF sponsor_json ? 'committee_id' THEN
        FOR v_elem IN SELECT jsonb_array_elements_text(sponsor_json->'committee_id') LOOP
            v_id := NULLIF(trim(v_elem), '')::INT;
            IF v_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM sponsors
                WHERE fk_bill_id = p_bill_id
                  AND fk_committee_id = v_id
            ) THEN
                INSERT INTO sponsors(sponsor_type, fk_committee_id, fk_bill_id)
                VALUES (sponsor_type::sponsor_type, v_id, p_bill_id);
            END IF;
        END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;
