#!/usr/bin/env bash
set -euo pipefail

# ---------------------------
# Configuration and Paths
# ---------------------------
PROJECT_ROOT=$(dirname "$(realpath "$0")")/..
ENV_FILE="$PROJECT_ROOT/.env"
# Primary SQL file for creating DBs and Roles (runs on default 'postgres' DB)
STRUCTURE_SQL="$PROJECT_ROOT/sql/bootstrap_db_and_users.sql"
# Secondary SQL file for creating schemas and tables (runs on main application DB)
SCHEMA_SQL="$PROJECT_ROOT/sql/create_schema_and_tables.sql"
UPDATE_HBA_SCRIPT="$PROJECT_ROOT/scripts/_update_pg_hba.sh"

# ---------------------------
# 1. Ensure Root Privileges
# ---------------------------
if [[ $EUID -ne 0 ]]; then
    echo "Re-running script with sudo to get administrative privileges..."
    exec sudo bash "$0" "$@"
fi

# ---------------------------
# 2. Load Environment Variables
# ---------------------------
echo "=== Loading environment variables ==="
if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi
set -a
source "$ENV_FILE"
set +a

# Helper function to run psql, passing all ENV vars as psql variables (-v)
psql_exec() {
    local db_name="${1:-postgres}" # Default to 'postgres'
    local sql_file="$2"
    echo "  -> Executing $sql_file against database: $db_name"

    sudo -u postgres psql -v ON_ERROR_STOP=1 \
        -v DB_NAME="$DB_NAME" \
        -v TEST_DB_NAME="$TEST_DB_NAME" \
        -v ADMIN_USER="$ADMIN_USER" \
        -v ADMIN_PASS="$ADMIN_PASS" \
        -v SCRAPER_USER="$SCRAPER_USER" \
        -v SCRAPER_PASS="$SCRAPER_PASS" \
        -v TEST_DB_USER="$TEST_DB_USER" \
        -v TEST_DB_PASS="$TEST_DB_PASS" \
        -v SCRAPER_SCHEMA="$SCRAPER_SCHEMA" \
        -d "$db_name" \
        -f "$sql_file"
}

# ---------------------------
# 3. Execution Flow
# ---------------------------

# 3a. Execute Structure Creation (Runs on 'postgres' DB)
echo "=== 1/3: Creating databases and roles ==="
psql_exec "postgres" "$STRUCTURE_SQL"

# 3b. Execute Schema/Table Creation (Runs on the new main DB)
echo "=== 2/3: Creating schema, tables, and grants on $DB_NAME ==="
# We pass the application DB name to the helper function
psql_exec "$DB_NAME" "$SCHEMA_SQL"

# 3c. Update pg_hba.conf (Required for Password Authentication)
echo "=== 3/3: Updating pg_hba.conf and reloading ==="
if [[ ! -x "$UPDATE_HBA_SCRIPT" ]]; then
    echo "Error: pg_hba update script not found or is not executable: $UPDATE_HBA_SCRIPT"
    exit 1
fi

# Run the update script for each user
"$UPDATE_HBA_SCRIPT" "$ADMIN_USER"
"$UPDATE_HBA_SCRIPT" "$SCRAPER_USER"
"$UPDATE_HBA_SCRIPT" "$TEST_DB_USER"

echo "=== Bootstrap finished successfully! ==="