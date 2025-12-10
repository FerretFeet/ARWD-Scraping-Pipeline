#!/usr/bin/env bash
set -euo pipefail

# ---------------------------
# Paths & Config
# ---------------------------
PROJECT_ROOT=$(dirname "$(realpath "$0")")/..
ENV_FILE="$PROJECT_ROOT/.env"
FUNCTIONS_DIR="$PROJECT_ROOT/sql/dml/functions"

# ---------------------------
# Load environment variables
# ---------------------------
echo "=== Loading environment variables ==="
if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found: $ENV_FILE"
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

# ---------------------------
# Helper to run SQL via psql
# ---------------------------
psql_exec() {
    local db_name="$1"
    local sql_file="$2"
    local db_user="$3"
    local db_pass="$4"

    echo "  -> Executing $sql_file on DB '$db_name' as '$db_user'"

    export PGPASSWORD="$db_pass"

    psql -v ON_ERROR_STOP=1 \
        -v SCRAPER_SCHEMA="$SCRAPER_SCHEMA" \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$db_user" \
        -d "$db_name" <<-EOSQL
            SET search_path TO $SCRAPER_SCHEMA;
            \i $sql_file
EOSQL

    unset PGPASSWORD
}


# ---------------------------
# Run all SQL functions
# ---------------------------
echo "=== Initializing SQL functions from: $FUNCTIONS_DIR ==="

if [[ ! -d "$FUNCTIONS_DIR" ]]; then
    echo "ERROR: Directory does not exist: $FUNCTIONS_DIR"
    exit 1
fi

shopt -s nullglob
function_files=("$FUNCTIONS_DIR"/*.sql)

if [[ ${#function_files[@]} -eq 0 ]]; then
    echo "No SQL files found in $FUNCTIONS_DIR"
    exit 0
fi

for sql_file in "${function_files[@]}"; do
    psql_exec "$DB_NAME" "$sql_file" "$ADMIN_USER" "$ADMIN_PASS"
done

echo "=== SQL function initialization complete ==="
