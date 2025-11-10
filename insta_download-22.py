# insta_download-2.py
import os
import re
import sys
import time
import math
import shutil
import argparse
from datetime import datetime, timezone

# ——— Ensure we run relative to this script's folder (so moving the whole root works) ———
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

from log_guard import already_logged_post

import instaloader
from instaloader import exceptions

# —————————————————————————————
# 0) CONFIG
# —————————————————————————————
PER_POST_SLEEP = 2.0  # seconds between posts to reduce 429s
MEDIA_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov"}
META_EXTS  = {".txt", ".json", ".xz", ".xml", ".log"}  # .json.xz ends with .xz
BACKOFF_BASE_SEC = 120   # 2m
BACKOFF_CAP_SEC  = 900   # 15m
MAX_RETRIES_PROFILE = 6  # backoff attempts for a throttled profile
MAX_RETRIES_POST    = 2  # attempts per post

# >>> PINNED POSTS HANDLING <<<
# Allow skipping this many already-seen posts at the very top (likely pinned/old).
MAX_LEADING_SEEN_SKIPS = 3

# >>> SESSION ROTATION <<<
# Rotate proactively every N newly-downloaded posts (0 = disable proactive rotation)
ROTATE_EVERY_POSTS = 12

# —————————————————————————————
# 1) CLEANUP: Fix any misnamed folders at root & under downloads/
# —————————————————————————————
def initial_cleanup():
    # Some runs created "downloads﹨<profile>" (unicode small reverse solidus)
    if os.path.exists("downloads"):
        for folder in os.listdir("downloads"):
            if "﹨" in folder:
                parts = folder.split("﹨", 1)
                profile = parts[1] if len(parts) > 1 else folder.replace("﹨", "")
                wrong = os.path.join("downloads", folder)
                correct = os.path.join("downloads", profile)
                if not os.path.exists(correct):
                    try:
                        shutil.move(wrong, correct)
                        print(f"🧹 Fixed '{folder}' → '{profile}' inside downloads/")
                    except Exception:
                        pass

    for item in os.listdir("."):
        if os.path.isdir(item) and "﹨" in item:
            parts = item.split("﹨", 1)
            if parts[0] == "downloads":
                profile = parts[1] if len(parts) > 1 else item.replace("downloads﹨", "")
                os.makedirs("downloads", exist_ok=True)
                src = item
                dst = os.path.join("downloads", profile)
                if not os.path.exists(dst):
                    try:
                        shutil.move(src, dst)
                        print(f"🧹 Moved root '{src}' → '{dst}'")
                    except Exception:
                        pass

# —————————————————————————————
# 2) SESSIONS (multi-session rotation)
# —————————————————————————————
def _parse_session_line(line: str) -> str:
    """
    Accepts either:
      - raw sessionid (e.g., 123456789%3Aabcdef...)
      - 'username|sessionid'
      - cookie-like 'sessionid=...;'
    Returns a sessionid string or '' if not found.
    """
    s = line.strip()
    if not s:
        return ""
    if "|" in s:  # username|sessionid
        parts = s.split("|", 1)
        return parts[1].strip()
    if "sessionid=" in s:
        # pull value between sessionid= and ; or end
        m = re.search(r"sessionid=([^;]+)", s)
        if m:
            return m.group(1).strip()
    return s

def load_sessions():
    """
    Order of preference:
      1) sessions.txt  (multiple lines, first used initially)
      2) session.txt   (single session id)
    Returns list[str] (>=1) or raises FileNotFoundError with guidance.
    """
    sessions_path = os.path.join(SCRIPT_DIR, "sessions.txt")
    single_path   = os.path.join(SCRIPT_DIR, "session.txt")

    sessions = []
    if os.path.exists(sessions_path):
        with open(sessions_path, "r", encoding="utf-8") as f:
            for ln in f:
                sid = _parse_session_line(ln)
                if sid:
                    sessions.append(sid)
    elif os.path.exists(single_path):
        with open(single_path, "r", encoding="utf-8") as f:
            s = _parse_session_line(f.read())
            if s:
                sessions = [s]

    if not sessions:
        raise FileNotFoundError(
            "⚠️  No session ids found. Create 'session.txt' (single line) "
            "or 'sessions.txt' (one sessionid per line)."
        )
    return sessions

def apply_session(L, sessionid: str):
    # reset & set cookie
    L.context._session.cookies.set("sessionid", sessionid)
    # iPhone-ish UA (helps reduce 401/429)
    L.context._session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) '
            'AppleWebKit/605.1.15 (KHTML, like Gecko) '
            'Mobile/15E148 Instagram 300.0.0.0.0'
        )
    })

def make_loader(initial_session: str):
    L = instaloader.Instaloader(
        download_comments=False,
        compress_json=True,
        post_metadata_txt_pattern="{shortcode}",
    )
    apply_session(L, initial_session)
    return L

def rotate_to_next_session(L, sessions, state):
    """
    Cycles to the next session. If we wrap around (exhausted all),
    signal the caller to back off before retrying.
    """
    prev = state["idx"]
    if state["idx"] + 1 < len(sessions):
        state["idx"] += 1
        apply_session(L, sessions[state["idx"]])
        print(f"🔁 Switched session {prev+1} → {state['idx']+1}")
        return False  # no wrap, no backoff necessary
    else:
        # wrap to first
        state["idx"] = 0
        apply_session(L, sessions[state["idx"]])
        print(f"🔁 Completed session cycle, wrapped to 1. Will back off.")
        return True  # wrapped, caller should back off

# —————————————————————————————
# 3) CLI / INPUT
# —————————————————————————————
def parse_args():
    p = argparse.ArgumentParser(description="Instagram downloader with session rotation and date window.")
    p.add_argument("profiles", nargs="*", help="Profile usernames (space or comma-separated). Falls back to profiles.txt.")
    p.add_argument("--before", dest="before", help="Download posts on or before YYYY-MM-DD.")
    p.add_argument("--after",  dest="after",  help="Download posts on or after  YYYY-MM-DD.")
    return p.parse_args()

def parse_profiles_from_cli(argv):
    items = []
    for tok in argv:
        if "," in tok:
            items.extend([p.strip() for p in tok.split(",") if p.strip()])
        else:
            tok = tok.strip()
            if tok:
                items.append(tok)
    return [p for p in items if p]

def load_profiles(args):
    profiles = parse_profiles_from_cli(args.profiles or [])
    if profiles:
        return profiles

    profiles_file = "profiles.txt"
    if not os.path.exists(profiles_file):
        raise FileNotFoundError(
            f"No profiles passed and {profiles_file} not found.\n"
            f"Usage:\n"
            f"  python {os.path.basename(__file__)} profile1\n"
            f"  python {os.path.basename(__file__)} profile1 profile2\n"
            f"  python {os.path.basename(__file__)} \"profile1,profile2\"\n"
            f"  (or provide {profiles_file})"
        )
    with open(profiles_file, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

def parse_date_utc(s):
    if not s:
        return None
    # YYYY-MM-DD → aware UTC midnight
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

# —————————————————————————————
# 4) HELPERS
# —————————————————————————————
def is_temp(fname: str) -> bool:
    return fname.endswith(".tmp") or fname.endswith(".part") or fname.endswith("~")

def ensure_dirs_for_profile(base_path: str):
    media_path = os.path.join(base_path, "media")
    meta_path  = os.path.join(base_path, "metadata")
    os.makedirs(media_path, exist_ok=True)
    os.makedirs(meta_path,  exist_ok=True)
    return media_path, meta_path

def any_mp4_exists_in_dirs(dirs, shortcode: str, basename: str) -> bool:
    sc_pat = re.compile(rf"^{re.escape(shortcode)}(?:_.+)?\.mp4$", re.IGNORECASE)
    bs_pat = re.compile(rf"^{re.escape(basename)}(?:_.+)?\.mp4$", re.IGNORECASE)
    for d in dirs:
        try:
            for fn in os.listdir(d):
                if sc_pat.match(fn) or bs_pat.match(fn):
                    return True
        except FileNotFoundError:
            pass
    return False

def move_sorted(base_path: str):
    media_path = os.path.join(base_path, "media")
    meta_path  = os.path.join(base_path, "metadata")
    os.makedirs(media_path, exist_ok=True)
    os.makedirs(meta_path,  exist_ok=True)

    for fname in os.listdir(base_path):
        full = os.path.join(base_path, fname)
        if os.path.isdir(full) or is_temp(fname):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext in MEDIA_EXTS:
            try:
                shutil.move(full, os.path.join(media_path, fname))
            except shutil.Error:
                pass
        elif ext in META_EXTS:
            try:
                shutil.move(full, os.path.join(meta_path, fname))
            except shutil.Error:
                pass

def backoff_wait(retry_idx: int, base_sec: int = BACKOFF_BASE_SEC, cap_sec: int = BACKOFF_CAP_SEC):
    wait = min(cap_sec, int(base_sec * (2 ** retry_idx)))
    print(f"⏳ Throttled/Unauthorized. Retrying in {wait//60}m {wait%60}s...")
    time.sleep(wait)

def to_utc(dt):
    """Return dt as timezone-aware UTC (handles naive/aware inputs)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def expected_basename_from_post(post) -> str:
    """
    Instaloader's default basename: YYYY-MM-DD_HH-MM-SS_UTC
    """
    dt = getattr(post, "date_utc", None)
    if dt is None:
        return ""
    dt = to_utc(dt)
    return dt.strftime("%Y-%m-%d_%H-%M-%S_UTC")

# —— Smarter mp4 existence checks to kill duplicates ——
def any_mp4_exists_for_post(dir_path: str, shortcode: str, basename: str) -> bool:
    sc_pat = re.compile(rf"^{re.escape(shortcode)}(?:_.+)?\.mp4$", re.IGNORECASE)
    bs_pat = re.compile(rf"^{re.escape(basename)}(?:_.+)?\.mp4$", re.IGNORECASE)
    try:
        for fn in os.listdir(dir_path):
            if sc_pat.match(fn) or bs_pat.match(fn):
                return True
    except FileNotFoundError:
        return False
    return False

def stream_save(session, url: str, dest_path: str) -> bool:
    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"   ↪️  Fallback download failed: {e}")
        return False

def ensure_post_videos(L, post, media_dir, base_path) -> int:
    rescued = 0
    session = L.context._session
    shortcode = getattr(post, "shortcode", None) or "unknown"
    basename  = expected_basename_from_post(post) or shortcode
    look_dirs = [media_dir, base_path]

    # Single video / reel
    if getattr(post, "is_video", False):
        if not any_mp4_exists_in_dirs(look_dirs, shortcode, basename) and getattr(post, "video_url", None):
            dest = os.path.join(media_dir, f"{basename}.mp4")
            print(f"   ↪️  No mp4 for {shortcode}. Trying fallback → {os.path.basename(dest)}")
            if stream_save(session, post.video_url, dest):
                rescued += 1

    # Sidecar videos — save each sidecar if its own file is missing (don’t block on main existing)
    try:
        if hasattr(post, "get_sidecar_nodes"):
            nodes = list(post.get_sidecar_nodes())
            for i, node in enumerate(nodes):
                if getattr(node, "is_video", False) and getattr(node, "video_url", None):
                    side_dest = os.path.join(media_dir, f"{basename}_{i+1}.mp4")
                    if not os.path.exists(side_dest):
                        print(f"   ↪️  Sidecar fallback {shortcode} [{i+1}] → {os.path.basename(side_dest)}")
                        if stream_save(session, node.video_url, side_dest):
                            rescued += 1
    except Exception as e:
        print(f"   ↪️  Sidecar probe failed: {e}")

    return rescued

# —————————————————————————————
# 5) MAIN DOWNLOAD LOOP
# —————————————————————————————
def main():
    args = parse_args()
    before_dt = parse_date_utc(args.before)
    after_dt  = parse_date_utc(args.after)
    use_window = (before_dt is not None) or (after_dt is not None)

    initial_cleanup()
    os.makedirs("downloads", exist_ok=True)

    sessions = load_sessions()
    sess_state = {"idx": 0}
    L = make_loader(sessions[sess_state["idx"]])

    profiles = load_profiles(args)
    total_rescued = 0

    def in_window(dt_utc):
        """Inclusive window check with UTC normalization."""
        if dt_utc is None:
            return False
        dt_utc = to_utc(dt_utc)
        if after_dt and dt_utc < after_dt:
            return False
        if before_dt and dt_utc > before_dt:
            return False
        return True

    try:
        for profile in profiles:
            print(f"\n📥 Starting download for: {profile}")
            base_path = os.path.join("downloads", profile)
            os.makedirs(base_path, exist_ok=True)
            media_path, meta_path = ensure_dirs_for_profile(base_path)
            log_file = os.path.join(meta_path, "skipped.log")

            retry = 0
            while retry < MAX_RETRIES_PROFILE:
                try:
                    inst_profile = instaloader.Profile.from_username(L.context, profile)

                    count_attempted = 0
                    rescued_here = 0
                    stop_this_profile = False

                    # >>> PINNED POSTS HANDLING <<<
                    leading_seen_skips = 0
                    downloaded_new_this_run = False

                    for post in inst_profile.get_posts():  # newest → oldest
                        dt = getattr(post, "date_utc", None)
                        dt_utc = to_utc(dt)
                        shortcode = getattr(post, "shortcode", None)
                        basename = expected_basename_from_post(post)

                        # If using a date window, skip fast until we reach it
                        if use_window and not in_window(dt_utc):
                            # too NEW for --before: keep skipping
                            if before_dt and dt_utc and dt_utc > before_dt:
                                continue
                            # too OLD for --after: everything next will be older → break
                            if after_dt and dt_utc and dt_utc < after_dt:
                                break
                            # default
                            continue

                        # Inside the desired window now — honor seen/stop logic
                        if already_logged_post(media_path, basename, shortcode):
                            if (not downloaded_new_this_run) and (leading_seen_skips < MAX_LEADING_SEEN_SKIPS):
                                leading_seen_skips += 1
                                print(f"📌 Leading seen #{leading_seen_skips} for {profile} "
                                      f"({basename or shortcode}). Skipping (likely pinned/old)…")
                                continue
                            print(f"⛔ Reached already-seen territory for {profile}: {basename or shortcode}. Stopping this profile.")
                            stop_this_profile = True
                            break

                        # ——— Normal download path for an unseen post ———
                        attempts = 0
                        while attempts < MAX_RETRIES_POST:
                            try:
                                # Instaloader downloads into base_path
                                L.download_post(post, target=base_path)
                                # Verify / fallback for reels & videos (check media dir to avoid dups)
                                rescued_here += ensure_post_videos(L, post, media_path, base_path)
                                count_attempted += 1
                                downloaded_new_this_run = True

                                # Proactive rotation every N posts if enabled
                                if ROTATE_EVERY_POSTS and (count_attempted % ROTATE_EVERY_POSTS == 0):
                                    wrapped = rotate_to_next_session(L, sessions, sess_state)
                                    if wrapped:
                                        backoff_wait(0)  # short pause when a full cycle completes

                                if PER_POST_SLEEP > 0:
                                    time.sleep(PER_POST_SLEEP)
                                break
                            except exceptions.ConnectionException as e:
                                attempts += 1
                                if attempts < MAX_RETRIES_POST:
                                    print(f"⚠️ Post {getattr(post,'shortcode','?')}: {e} → retrying in 10s ({attempts}/{MAX_RETRIES_POST})")
                                    time.sleep(10)
                                else:
                                    print(f"❌ Skipping post {getattr(post,'shortcode','?')} after {MAX_RETRIES_POST} attempts: {e}")
                                    try:
                                        with open(log_file, "a", encoding="utf-8") as logf:
                                            logf.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {getattr(post,'shortcode','?')}  {e}\n")
                                    except Exception:
                                        pass
                                    break

                    print(f"✅ Download phase done ({count_attempted} posts attempted, {rescued_here} videos rescued). Organizing files…")
                    move_sorted(base_path)
                    print(f"✅ Finished profile: {profile}")
                    total_rescued += rescued_here
                    break  # done with this profile (don’t retry profile-level loop)

                except exceptions.ProfileNotExistsException:
                    print(f"🚫 Profile not found: {profile}. Skipping.")
                    break
                except exceptions.PrivateProfileNotFollowedException:
                    print(f"🔒 Private profile (not followed): {profile}. Skipping.")
                    break
                except exceptions.ConnectionException as e:
                    msg = str(e)
                    # If auth/throttle-ish → rotate session first, then backoff/retry
                    if ("401" in msg) or ("403" in msg) or ("429" in msg) or ("Please wait a few minutes" in msg):
                        wrapped = rotate_to_next_session(L, sessions, sess_state)
                        if wrapped:
                            backoff_wait(retry)
                            retry += 1
                        # continue loop with new session (no immediate backoff if not wrapped)
                        continue
                    # otherwise re-raise unknown connection errors
                    raise

        print(f"\n🎉 All done! Total rescued videos via fallback: {total_rescued}")

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user. Organizing any downloaded files before exit…")
        for profile in profiles:
            base_path = os.path.join("downloads", profile)
            if os.path.isdir(base_path):
                move_sorted(base_path)
        print("👋 Bye.")

# —————————————————————————————
# Entry
# —————————————————————————————
if __name__ == "__main__":
    main()
