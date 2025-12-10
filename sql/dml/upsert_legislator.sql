CREATE OR REPLACE FUNCTION upsert_legislator(
    p_first_name VARCHAR,
    p_last_name VARCHAR,
    p_url TEXT,
    p_phone VARCHAR,
    p_email VARCHAR,
    p_address TEXT,
    p_district VARCHAR,
    p_seniority INT,
    p_chamber chamber,
    p_party VARCHAR,
    p_start_date DATE,
    p_committee_ids INT[]
) RETURNS INT AS $$
DECLARE
    v_legislator_id INT;
    v_history_id INT;
    v_existing_legislator_id INT;
    v_target_committee_id INT;
BEGIN
    -- STEP 1: Try to find an existing legislator based on name and URL (Identity Resolution)
    SELECT legislator_id INTO v_existing_legislator_id
    FROM legislators
    WHERE first_name = p_first_name
      AND last_name = p_last_name
    LIMIT 1;

    IF v_existing_legislator_id IS NOT NULL THEN
        -- Legislator found: Update the main record (SCD Type 1)
        v_legislator_id := v_existing_legislator_id;

        UPDATE legislators
        SET phone = p_phone,
            email = p_email,
            address = p_address
        WHERE legislator_id = v_legislator_id;
    ELSE
        -- Legislator not found: Insert a brand new legislator record
        INSERT INTO legislators (first_name, last_name, phone, email, address)
        VALUES (p_first_name, p_last_name, p_phone, p_email, p_address)
        RETURNING legislator_id INTO v_legislator_id;
    END IF;

    -- STEP 1.5: Insert Committee And/or Committee Memberships
    -- Close all memberships not in current list that have an open record before cur session
UPDATE committee_membership
SET membership_end = p_start_date - INTERVAL '1 day'
WHERE fk_legislator_id = v_legislator_id
  AND membership_end IS NULL
  AND (
        p_committee_ids IS NULL
        OR fk_committee_id <> ALL(p_committee_ids)
      )
  AND membership_start < p_start_date;
-- Insert new memberships (safe even if array is NULL)
FOREACH v_target_committee_id IN ARRAY COALESCE(p_committee_ids, ARRAY[]::integer[])
LOOP
    -- Only insert if no open membership already exists
    IF NOT EXISTS (
        SELECT 1
        FROM committee_membership
        WHERE fk_legislator_id = v_legislator_id
          AND fk_committee_id = v_target_committee_id
          AND membership_end IS NULL
    ) THEN
        INSERT INTO committee_membership (
            fk_committee_id,
            fk_legislator_id,
            membership_start
        ) VALUES (
            v_target_committee_id,
            v_legislator_id,
            p_start_date
        );
    END IF;
END LOOP;
    -- STEP 2: Handle the History (SCD Type 2 Logic)
    -- Check for the current (most recent and open) history record
    SELECT history_id INTO v_history_id
    FROM legislator_history
    WHERE fk_legislator_id = v_legislator_id
      AND end_date IS NULL
    ORDER BY start_date DESC
    LIMIT 1;

    IF v_history_id IS NOT NULL THEN
        -- Check if the new data represents a change in office (party, chamber, district) or to url
        IF EXISTS (
            SELECT 1 FROM legislator_history
            WHERE history_id = v_history_id
              AND (
                  (p_party IS DISTINCT FROM party) OR
                  (p_chamber IS DISTINCT FROM chamber) OR
                  (p_district IS DISTINCT FROM district) OR
                  (p_url IS DISTINCT FROM url)
              )
        ) THEN
            -- Data changed (New Term/Role): Close the old history record
            UPDATE legislator_history
            SET end_date = p_start_date - INTERVAL '1 day'
            WHERE history_id = v_history_id
                AND start_date < p_start_date;
        ELSE
            -- No major change: Exit, we don't need to insert a new history record
            RETURN v_legislator_id;
        END IF;
    END IF;

    -- STEP 3: Insert the new (current) history record
    INSERT INTO legislator_history (fk_legislator_id, district, seniority, chamber, url, party, start_date, end_date)
    VALUES (v_legislator_id, p_district, p_seniority, p_chamber, p_url, p_party, p_start_date, NULL);

    RETURN v_legislator_id;

END;
$$ LANGUAGE plpgsql;
