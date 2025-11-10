#!/usr/bin/env bash
set -euo pipefail

BASE="/srv/igdl"
LOGDIR="$BASE/logs"
RAW="$LOGDIR/last_raw.log"
SUMMARY="$BASE/server_last_run.log"
TMUX_SESSION="igdl-squad"

mkdir -p "$LOGDIR"
: > "$RAW"
: > "$SUMMARY"

cd "$BASE"

{
  echo "=== START $(date) ==="
  echo "[1/4] fetch_sheets.py"
  python3 fetch_sheets.py
  echo

  echo "[2/4] Launch parallel workers (even split across ALL sessions)"
  # Optional per-run args to pass through to the downloader:
  #   export DL_ARGS="--after 2025-01-01 --before 2025-09-01"
  ./igdl-squad.sh
  echo

  echo "[3/4] Attaching to tmux ($TMUX_SESSION) — close when done."
} 2>&1 | tee -a "$RAW"

# Attach to live dashboard (workers + KPI). When tmux is closed, proceed.
tmux attach -t "$TMUX_SESSION" || true

# Safety wait in case of detach-before-finish
echo "[i] Waiting for tmux session to end…"
while tmux has-session -t "$TMUX_SESSION" 2>/dev/null; do
  sleep 5
done

# Merge worker logs for summary
echo "[4/4] Summarizing logs" | tee -a "$RAW"
if ls "$LOGDIR"/worker*.log >/dev/null 2>&1; then
  cat "$LOGDIR"/worker*.log >> "$RAW" || true
fi

# Summarize (ignore failure)
if [[ -x "$BASE/bin/summarize_insta_log.py" ]]; then
  "$BASE/bin/summarize_insta_log.py" "$RAW" > "$SUMMARY" || true
fi

echo "[i] Consolidating worker downloads into /srv/igdl/"
/srv/igdl/consolidate_worker_downloads.sh || true

# Cleanup scaffolding
./cleanup-squad.sh

# Housekeeping
find "$BASE/downloads" -type f -name "*.tmp" -mtime +14 -delete >/dev/null 2>&1 || true

echo "=== END $(date) ===" | tee -a "$RAW"
