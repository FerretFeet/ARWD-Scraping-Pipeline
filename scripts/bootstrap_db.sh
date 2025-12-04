#!/usr/bin/env bash
set -euo pipefail

# ---------------------------
# Configuration
# ---------------------------
PROJECT_ROOT=$(dirname "$(realpath "$0")")/..
ENV_FILE="$PROJECT_ROOT/.env"

# SQL files
ENUMS_SQL="$PROJECT_ROOT/db/ddl/enums.sql"
TABLES_SQL="$PROJECT_ROOT/db/ddl/tables.sql"

# Required variables in .env
REQUIRED_VARS=("DB_NAME" "ADMIN_USER" "SCRAPER_USER" "SCRAPER_SCHEMA")

# If not root, re-run script with sudo
if [[ $EUID -ne 0 ]]; then
    echo "Re-running script with sudo to get administrative privileges..."
    exec sudo bash "$0" "$@"
fi

# ---------------------------
# Load environment variables
# ---------------------------
if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

while IFS='=' read -r key value; do
    [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
    for req in "${REQUIRED_VARS[@]}"; do
        if [[ "$key" == "$req" ]]; then
            export $key="$value"
        fi
    done
done < "$ENV_FILE"

# Validate required vars
for req in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!req:-}" ]]; then
        echo "Error: Required environment variable '$req' not set in $ENV_FILE"
        exit 1
    fi
done

# ---------------------------
# Ensure OS 'postgres' exists
# ---------------------------
if ! id postgres &>/dev/null; then
    echo "PostgreSQL OS user 'postgres' not found. Install PostgreSQL first."
    exit 1
fi

# ---------------------------
# Helper functions
# ---------------------------
# Run SQL as OS postgres user
psql_postgres() {
    sudo -u postgres psql -v ON_ERROR_STOP=1 "$@"
}

# Run SQL file as PostgreSQL role with matching OS user, setting search_path
psql_as_user() {
    local user="$1"
    local db="$2"
    local sql_file="$3"
    echo "Running SQL file: $sql_file as $user on $db"

    sudo -u "$user" psql -d "$db" -v SCRAPER_SCHEMA="$SCRAPER_SCHEMA" <<-EOSQL
        SET search_path TO :'SCRAPER_SCHEMA';
        \i $sql_file
EOSQL
}

# Create PostgreSQL role if missing
create_role_if_missing() {
    local role="$1"
    echo "Creating role if missing: $role"
    if ! psql_postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$role'" | grep -q 1; then
        psql_postgres -c "CREATE ROLE $role LOGIN;"
    fi
}

# Create database if missing
create_db_if_missing() {
    local db="$1"
    local owner="$2"
    echo "Creating database if missing: $db"
    if ! psql_postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1; then
        psql_postgres -c "CREATE DATABASE $db OWNER $owner ENCODING 'UTF8';"
        psql_postgres -d "$db" -c "ALTER DATABASE $db SET timezone TO 'UTC';"
    fi
}

# Create schema if missing
create_schema_if_missing() {
    local db="$1"
    local schema="$2"
    local owner="$3"
    echo "Creating schema if missing: $schema in $db"
    psql_postgres -d "$db" -c "CREATE SCHEMA IF NOT EXISTS $schema AUTHORIZATION $owner;"
    psql_postgres -d "$db" -c "GRANT ALL PRIVILEGES ON SCHEMA $schema TO $owner;"
}

# Create matching OS user if missing
create_os_user_if_missing() {
    local user="$1"
    if ! id "$user" &>/dev/null; then
        echo "Creating OS user $user..."
        sudo useradd -r -m -s /usr/sbin/nologin "$user"
    fi
}

# ---------------------------
# 1. Ensure OS users exist
# ---------------------------
create_os_user_if_missing "$ADMIN_USER"
create_os_user_if_missing "$SCRAPER_USER"

# ---------------------------
# 2. Create PostgreSQL roles and database
# ---------------------------
echo "=== Creating PostgreSQL roles and database ==="
create_role_if_missing "$ADMIN_USER"
create_role_if_missing "$SCRAPER_USER"
create_db_if_missing "$DB_NAME" "$ADMIN_USER"

# ---------------------------
# 3. Create schema
# ---------------------------
echo "=== Creating schema ==="
create_schema_if_missing "$DB_NAME" "$SCRAPER_SCHEMA" "$SCRAPER_USER"

# ---------------------------
# 4. Run enums and tables
# ---------------------------
echo "=== Running enums and tables SQL ==="
psql_as_user "$SCRAPER_USER" "$DB_NAME" "$ENUMS_SQL"
psql_as_user "$SCRAPER_USER" "$DB_NAME" "$TABLES_SQL"

echo "=== Bootstrap finished successfully ==="
