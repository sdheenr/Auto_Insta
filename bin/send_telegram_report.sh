#!/usr/bin/env bash
set -euo pipefail
# Load bot creds if present
if [ -f /etc/profile.d/igdl_telegram.sh ]; then
  . /etc/profile.d/igdl_telegram.sh
fi
exec /usr/bin/env python3 /srv/igdl/bin/telegram_daily_report.py
