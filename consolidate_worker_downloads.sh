#!/usr/bin/env bash
set -euo pipefail

ROOT="/srv/igdl"
WORKERS_DIR="$ROOT/workers"
TARGET_ROOT="$ROOT"   # move folders directly under /srv/igdl

DRY="${DRY_RUN:-0}"   # set DRY_RUN=1 to preview actions

# collect candidate dirs: names starting with "downloads" (any case), within each worker
mapfile -t DL_DIRS < <(find "$WORKERS_DIR" -maxdepth 2 -mindepth 2 -type d -iname 'downloads*' 2>/dev/null | sort)

if [[ ${#DL_DIRS[@]} -eq 0 ]]; then
  echo "[move] No worker downloads* folders found."
  exit 0
fi

echo "[move] Found ${#DL_DIRS[@]} folder(s) to consider. Target root: $TARGET_ROOT"
for D in "${DL_DIRS[@]}"; do
  base="$(basename "$D")"
  parent="$(basename "$(dirname "$D")")"   # e.g., worker01
  base_lc="$(printf '%s' "$base" | tr '[:upper:]' '[:lower:]')"

  # Skip exact 'downloads'
  if [[ "$base_lc" == "downloads" ]]; then
    echo "[move] Skip exact 'downloads': $D"
    continue
  fi

  # Skip symlink and mountpoint
  if [[ -L "$D" ]]; then
    echo "[move] Skip symlink: $D -> $(readlink -f "$D")"
    continue
  fi
  if mountpoint -q "$D"; then
    echo "[move] Skip mountpoint: $D"
    continue
  fi

  # Decide destination; ensure uniqueness if name already exists
  dest="$TARGET_ROOT/$base"
  if [[ -e "$dest" ]]; then
    dest="${TARGET_ROOT}/${base}__${parent}"
    # keep trying until unique
    while [[ -e "$dest" ]]; do
      dest="${TARGET_ROOT}/${base}__${parent}__$RANDOM"
    done
  fi

  if [[ "$DRY" == "1" ]]; then
    echo "[dry] mv '$D' -> '$dest'"
  else
    echo "[move] mv '$D' -> '$dest'"
    mv "$D" "$dest"
  fi
done

echo "[move] Done."
