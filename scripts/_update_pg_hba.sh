#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <ROLE_NAME>"
    exit 1
fi

ROLE="$1"

# Find PostgreSQL configuration directory
PG_CONF_DIR=$(sudo -u postgres psql -t -P format=unaligned -c "SHOW config_file" | xargs dirname)
PG_HBA="$PG_CONF_DIR/pg_hba.conf"

echo "Updating pg_hba.conf for ROLE=$ROLE..."

# Backup
sudo cp "$PG_HBA" "$PG_HBA.bak"

# Check if md5 line already exists for this role
if ! grep -qE "^host\s+all\s+$ROLE\s+127\.0\.0\.1/32\s+md5" "$PG_HBA"; then
    # Insert before first generic ident line
    sudo sed -i "/^host\s\+all\s\+all\s\+127\.0\.0\.1\/32\s\+ident/i host    all    $ROLE    127.0.0.1/32    md5" "$PG_HBA"
    echo "Added md5 line for $ROLE"
else
    echo "md5 line already exists for $ROLE"
fi

# Optionally, also add IPv6 line
if ! grep -qE "^host\s+all\s+$ROLE\s+::1/128\s+md5" "$PG_HBA"; then
    sudo sed -i "/^host\s\+all\s\+all\s\+::1\/128\s\+ident/i host    all    $ROLE    ::1/128    md5" "$PG_HBA"
    echo "Added IPv6 md5 line for $ROLE"
else
    echo "IPv6 md5 line already exists for $ROLE"
fi

# Reload PostgreSQL configuration
sudo systemctl reload postgresql

echo "pg_hba.conf updated successfully."
