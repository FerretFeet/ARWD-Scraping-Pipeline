CREATE OR REPLACE FUNCTION insert_session(
    p_session_code VARCHAR,
    p_name TEXT,
    p_start_date DATE
)
RETURNS VARCHAR AS $$
    WITH ins AS (
        INSERT INTO sessions (session_code, session_name, start_date)
        VALUES (p_session_code, p_name, p_start_date)
        ON CONFLICT (session_code) DO NOTHING
        RETURNING session_code
    )
    SELECT COALESCE(
        (SELECT session_code FROM ins),
        p_session_code
    );
$$ LANGUAGE sql;