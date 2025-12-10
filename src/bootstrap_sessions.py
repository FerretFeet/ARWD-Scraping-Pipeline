import json

import psycopg

from src.utils.paths import project_root

session_dict_path = project_root / "sessions_dict.json"
session_insert_path = project_root/ "sql" / "dml" / "functions" / "insert_session.sql"


# Load JSON data
with open(session_dict_path) as f:
    sessions_data = json.load(f)

# Load SQL function from file
with open(session_insert_path) as f:
    sql_function = f.read()

def insert_sessions(conn, data: dict, sql_func: str):
    with conn.cursor() as cur:
        # Ensure the function exists
        cur.execute(sql_func)

        # Insert each session by calling the SQL function
        for code, info in data.items():
            cur.execute(
                "SELECT insert_session(%s, %s, %s);",
                (code, info["label"], info["year"]),
            )
        conn.commit()
        cur.execute("SELECT session_code FROM sessions ORDER BY start_date ASC;")
        result = cur.fetchall()
        return result


# Usage
if __name__ == "__main__":
    with psycopg.connect("dbname=yourdb user=youruser password=yourpass host=localhost") as conn:
        insert_sessions(conn, sessions_data, sql_function)
