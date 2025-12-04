#!/usr/bin/env bash
set -euo pipefail

SCRAPER_USER="$1"

# Find the directory of the active pg_hba.conf
PG_CONF_DIR=$(sudo -u postgres psql -t -P format=unaligned -c "SHOW config_file" | xargs dirname)
PG_HBA="$PG_CONF_DIR/pg_hba.conf"

echo "Updating pg_hba.conf for SCRAPER_USER=$SCRAPER_USER..."

# Backup
sudo cp "$PG_HBA" "$PG_HBA.bak"

# Append line if it doesn’t exist
grep -q "^host\s\+all\s\+$SCRAPER_USER\s\+127\.0\.0\.1/32\s\+md5" "$PG_HBA" || \
    echo "host    all    $SCRAPER_USER    127.0.0.1/32    md5" | sudo tee -a "$PG_HBA" >/dev/null

# Reload PostgreSQL
sudo systemctl reload postgresql

echo "pg_hba.conf updated successfully."
