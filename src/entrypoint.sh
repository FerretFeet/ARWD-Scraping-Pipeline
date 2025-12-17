#!/bin/sh

# This runs *after* the volume is mounted, correcting the host permissions.
echo "Setting universal write permissions for mounted volumes..."
chmod -R 777 /app/logs
chmod -R 777 /app/data/pdfs

# Execute the main application command.
# 'exec' replaces the shell process with the Python process, saving resources.
echo "Starting scraper application..."
exec python -u -m src.main