CREATE OR REPLACE FUNCTION upsert_committee(
    p_name TEXT,
    p_url TEXT
) RETURNS INT AS $$
    INSERT INTO committees (name, url)
    VALUES (p_name, p_url)
    ON CONFLICT (name) DO UPDATE
    SET
        -- If a conflict occurs, update the URL. COALESCE keeps the existing
        -- URL if the new one (EXCLUDED.url) is passed as NULL.
        url = COALESCE(EXCLUDED.url, committees.url)
    RETURNING committee_id;
$$ LANGUAGE sql;