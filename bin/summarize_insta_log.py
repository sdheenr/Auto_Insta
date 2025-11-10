#!/usr/bin/env python3
import re, sys
from datetime import datetime

raw_path = sys.argv[1] if len(sys.argv) > 1 else None
if not raw_path:
    print("Usage: summarize_insta_log.py /srv/igdl/logs/last_raw.log", file=sys.stderr)
    sys.exit(1)

re_done = re.compile(r'Download phase done\s*\((\d+)\s+posts attempted,\s*(\d+)\s+videos rescued\)', re.IGNORECASE)
re_finished = re.compile(r'Finished profile:\s*(.+)$', re.IGNORECASE)

posts_attempted = None
videos_rescued = None
summaries = []

with open(raw_path, 'r', encoding='utf-8', errors='replace') as f:
    for line in f:
        line = line.strip()
        m_done = re_done.search(line)
        if m_done:
            posts_attempted = int(m_done.group(1))
            videos_rescued = int(m_done.group(2))
            continue
        m_fin = re_finished.search(line)
        if m_fin:
            profile = m_fin.group(1).strip()
            if posts_attempted is not None and videos_rescued is not None:
                if posts_attempted > 0 or videos_rescued > 0:
                    summaries.append(f"{profile} : {posts_attempted} posts attempted, {videos_rescued} videos rescued")
            posts_attempted = None
            videos_rescued = None

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Last run (server local time): {now}")
print("---- Summary per profile ----")
if summaries:
    for s in summaries:
        print(s)
else:
    print("(No non-zero profile summaries found.)")
