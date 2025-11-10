#!/usr/bin/env bash
set -euo pipefail
ROOT="/srv/igdl"
TMUX_SESSION="igdl-squad"
WORKERS_DIR="$ROOT/workers"

tmux has-session -t "$TMUX_SESSION" 2>/dev/null && tmux kill-session -t "$TMUX_SESSION" || true
rm -rf "$WORKERS_DIR"/*
echo "🧹 Cleaned workers/. (Logs remain in $ROOT/logs)"
