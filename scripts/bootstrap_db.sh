#!/usr/bin/env bash
set -euo pipefail

# ---------------------------
# Configuration and Paths
# ---------------------------
PROJECT_ROOT=$(dirname "$(realpath "$0")")/..
ENV_FILE="$PROJECT_ROOT/.env"
STRUCTURE_SQL="$PROJECT_ROOT/sql/bootstrap_db_and_users.sql"
ENUMS_SQL="$PROJECT_ROOT/sql/ddl/enums.sql"
TABLES_SQL="$PROJECT_ROOT/sql/ddl/tables.sql"
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

# ---------------------------
# Helper function to run SQL via psql
# ---------------------------
psql_exec() {
    local db_name="${1:-postgres}"
    local sql_file="$2"
    local db_user="${3:-postgres}"
    local db_pass="${4:-}"

    echo "  -> Executing $sql_file against database: $db_name as user: $db_user"

    if [[ -n "$db_pass" ]]; then
        export PGPASSWORD="$db_pass"
    fi

    # Run SQL with search_path set and pass all variables for SQL substitution
    sudo -u "$db_user" psql -v ON_ERROR_STOP=1 \
        -v DB_NAME="$DB_NAME" \
        -v TEST_DB_NAME="$TEST_DB_NAME" \
        -v ADMIN_USER="$ADMIN_USER" \
        -v ADMIN_PASS="$ADMIN_PASS" \
        -v SCRAPER_USER="$SCRAPER_USER" \
        -v SCRAPER_PASS="$SCRAPER_PASS" \
        -v TEST_DB_USER="$TEST_DB_USER" \
        -v TEST_DB_PASS="$TEST_DB_PASS" \
        -v SCRAPER_SCHEMA="$SCRAPER_SCHEMA" \
        -d "$db_name" <<-EOSQL
            SET search_path TO $SCRAPER_SCHEMA;
            \i $sql_file
EOSQL

    if [[ -n "$db_pass" ]]; then
        unset PGPASSWORD
    fi
}

# ---------------------------
# 3. Execution Flow
# ---------------------------

# 3a. Execute Structure Creation (Runs on 'postgres' DB)
echo "=== 1/3: Creating databases and roles ==="
psql_exec "postgres" "$STRUCTURE_SQL"

# 3b. Execute Schema/Table Creation (Runs on main application DB)
echo "=== 2/3: Creating schema, tables, and grants on $DB_NAME ==="
psql_exec "$DB_NAME" "$ENUMS_SQL" "$SCRAPER_USER" "$SCRAPER_PASS"
psql_exec "$DB_NAME" "$TABLES_SQL" "$SCRAPER_USER" "$SCRAPER_PASS"

# 3c. Update pg_hba.conf
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
