#!/usr/bin/env bash
set -euo pipefail

ROOT="/srv/igdl"
SCRIPT="insta_download-22.py"
GUARD="log_guard.py"
MASTER_PROFILES="$ROOT/profiles.txt"
MASTER_SESSIONS="$ROOT/sessions.txt"
DOWNLOADS="$ROOT/downloads"
WORKERS_DIR="$ROOT/workers"
LOGDIR="$ROOT/logs"
TMUX_SESSION="igdl-squad"
PY="${PY:-python3}"
DL_ARGS="${DL_ARGS:-}"     # e.g. --after 2025-01-01

cd "$ROOT"
mkdir -p "$WORKERS_DIR" "$LOGDIR" "$DOWNLOADS"

[[ -s "$MASTER_PROFILES" ]]  || { echo "profiles.txt missing/empty"; exit 1; }
[[ -s "$MASTER_SESSIONS" ]]  || { echo "sessions.txt missing/empty"; exit 1; }
[[ -d "$DOWNLOADS" ]]        || { echo "downloads/ missing"; exit 1; }
[[ -f "$SCRIPT" && -f "$GUARD" ]] || { echo "Need $SCRIPT and $GUARD in $ROOT"; exit 1; }

# Read sessions & profiles
mapfile -t SESSIONS < <(grep -v '^[[:space:]]*$' "$MASTER_SESSIONS")
NUM_SESS=${#SESSIONS[@]}
(( NUM_SESS > 0 )) || { echo "No sessions found"; exit 1; }

mapfile -t PROFILES < <(grep -v '^[[:space:]]*$' "$MASTER_PROFILES")
NPROF=${#PROFILES[@]}
(( NPROF > 0 )) || { echo "No profiles in profiles.txt"; exit 1; }

echo "→ Workers (sessions): $NUM_SESS"
echo "→ Profiles: $NPROF | Split: even"

# Reset workers dir
rm -rf "$WORKERS_DIR"/*
for i in $(seq 1 $NUM_SESS); do
  W="worker$(printf '%02d' "$i")"
  WP="$WORKERS_DIR/$W"
  mkdir -p "$WP"
  cp "$SCRIPT" "$GUARD" "$WP/"
  ln -sfn "$DOWNLOADS" "$WP/downloads"
done

# Even contiguous chunks
CHUNK=$(( (NPROF + NUM_SESS - 1) / NUM_SESS ))
for i in $(seq 1 $NUM_SESS); do
  s=$(( (i-1)*CHUNK ))
  e=$(( s + CHUNK - 1 )); (( e >= NPROF )) && e=$(( NPROF - 1 ))
  W="worker$(printf '%02d' "$i")"
  WP="$WORKERS_DIR/$W"
  if (( s <= e )); then
    printf "%s\n" "${PROFILES[@]:$s:$((e-s+1))}" > "$WP/profiles.txt"
  else
    : > "$WP/profiles.txt"
  fi
  echo "${SESSIONS[$((i-1))]}" > "$WP/session.txt"
done

# Run marker (for KPI to count "new files since start")
MARKER="$WORKERS_DIR/.run_marker"
: > "$MARKER"
touch -d "@$(date +%s)" "$MARKER" || true

# Write KPI helper script next to workers
KPI="$WORKERS_DIR/kpi_dashboard.sh"
cat > "$KPI" <<'KPI_EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT="/srv/igdl"
WORKERS_DIR="$ROOT/workers"
LOGDIR="$ROOT/logs"
MARKER="$WORKERS_DIR/.run_marker"

# Simple looped dashboard
while true; do
  clear
  echo "📊 IGDL Live KPI — $(date '+%Y-%m-%d %H:%M:%S')"
  echo "Marker: $MARKER"
  echo

  # Totals since marker
  if [[ -f "$MARKER" ]]; then
    TOTAL=$(find "$ROOT/downloads" -type f -newer "$MARKER" 2>/dev/null | wc -l)
  else
    TOTAL="n/a"
  fi
  echo "Total new files since start: $TOTAL"
  echo

  # Per-worker tally (sum of files in that worker's profiles since marker)
  printf "%-12s | %-8s | %s\n" "Worker" "NewFiles" "Last log line"
  printf -- "-----------------------------------------------\n"
  for WP in "$WORKERS_DIR"/worker*/; do
    [[ -d "$WP" ]] || continue
    W=$(basename "$WP")
    SUM=0
    if [[ -f "$WP/profiles.txt" && -f "$MARKER" ]]; then
      # Sum all profiles for this worker
      while IFS= read -r P || [[ -n "$P" ]]; do
        [[ -d "$ROOT/downloads/$P" ]] || continue
        C=$(find "$ROOT/downloads/$P" -type f -newer "$MARKER" 2>/dev/null | wc -l)
        SUM=$((SUM + C))
      done < "$WP/profiles.txt"
    fi
    LAST=$(tail -n 1 "$LOGDIR/${W}.log" 2>/dev/null || echo "-")
    printf "%-12s | %-8s | %s\n" "$W" "$SUM" "$LAST"
  done

  echo
  echo "(refreshing… Ctrl+C to exit this pane)"
  sleep 5
done
KPI_EOF
chmod +x "$KPI"

# (re)start tmux
tmux has-session -t "$TMUX_SESSION" 2>/dev/null && tmux kill-session -t "$TMUX_SESSION"
tmux new-session -d -s "$TMUX_SESSION"

# Launch one pane per worker
for i in $(seq 1 $NUM_SESS); do
  W="worker$(printf '%02d' "$i")"
  CMD="cd '$WORKERS_DIR/$W' && ( date; echo '=== $W ==='; $PY insta_download-22.py $DL_ARGS ) 2>&1 | tee -a '$LOGDIR/$W.log'"
  if (( i == 1 )); then
    tmux send-keys -t "$TMUX_SESSION" "$CMD" C-m
  else
    tmux split-window -t "$TMUX_SESSION" -h
    tmux send-keys -t "$TMUX_SESSION" "$CMD" C-m
    tmux select-layout -t "$TMUX_SESSION" tiled >/dev/null
  fi
done

# Add KPI pane
tmux split-window -t "$TMUX_SESSION" -v
tmux send-keys -t "$TMUX_SESSION" "$KPI" C-m
tmux select-layout -t "$TMUX_SESSION" tiled >/dev/null

# Make view nicer
tmux set -t "$TMUX_SESSION" -g mouse on
tmux set -t "$TMUX_SESSION" -g pane-border-status top
tmux set -t "$TMUX_SESSION" -g pane-border-format "#[bold] #{pane_index} #{pane_current_path:t}"
tmux set -t "$TMUX_SESSION" -g status-right "📦 #{session_windows} | 🕒 %Y-%m-%d %H:%M"

echo "✅ tmux session ready: $TMUX_SESSION (workers + KPI)"
