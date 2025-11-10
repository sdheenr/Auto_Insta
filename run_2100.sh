#!/usr/bin/env bash
set -euo pipefail

BASE="/srv/igdl"
LOGDIR="$BASE/logs"
RAW="$LOGDIR/last_raw.log"
SUMMARY="$BASE/server_last_run.log"

mkdir -p "$LOGDIR"
: > "$RAW"
: > "$SUMMARY"

cd "$BASE"

{
  echo "=== START $(date) ==="
  echo "[1/2] fetch_sheets.py"
  python3 fetch_sheets.py
  echo
  echo "[2/2] insta_download-22.py"
  python3 insta_download-22.py
  echo
  echo "=== END $(date) ==="
} 2>&1 | tee "$RAW"

"$BASE/bin/summarize_insta_log.py" "$RAW" > "$SUMMARY"

# Housekeeping
find "$BASE/downloads" -type f -name "*.tmp" -mtime +14 -delete >/dev/null 2>&1 || true
