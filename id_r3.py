# insta_download_unified.py (r3)
#
# ✅ Unified script implementing marker-based early stop (daily) + bulk init
#    - *Daily mode*: boundary stop using per-stream latest_seen markers
#    - *Init mode*: crawl history (optionally bounded by --after/--before)
#    - *All mode*: like daily but also pulls stories & highlights
#
# ✅ Session management
#    - sessions.txt cookie handling (supports sessionid or cookie string)
#    - Time-based session rotation across the whole run
#    - Error-based rotation on 403/429/login issues
#
# ✅ Robustness & speed
#    - Duplicate-safe fallback video rescue (checks base + media dirs)
#    - Log backfill from disk (so daily won’t re-fetch old posts)
#    - Consecutive-seen streak to bail fast when nothing new
#    - Date windows with strict edges (pdt > --after, pdt < --before)
#    - Bounded probes avoid endless 403 spam
#
# USAGE
#   python insta_download_unified.py daily some_profile --feed-only
#   python insta_download_unified.py daily -f profiles.txt --reels-only --rotate-interval 90
#   python insta_download_unified.py init some_profile --after 2024-08-01
#   python insta_download_unified.py all some_profile  # includes stories & highlights
#
# Marker files (per profile, per stream):
#   downloads/<profile>/metadata/latest_seen_feed.json
#   downloads/<profile>/metadata/latest_seen_reels.json
#   Format: {"ts":"YYYY-MM-DDTHH:MM:SS+00:00","shortcode":"abc123"}

print("▶ INSTADL unified build r3 (probes+marker-seed+safe-unpack)")

import os
import sys
import time
import shutil
import re
import json
from datetime import datetime, timezone
from typing import Iterable, Tuple, List, Dict, Optional
import argparse

import instaloader
from instaloader import exceptions

# ——— Run relative to this script ———
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

from log_guard import already_logged_post  # must exist in the same folder

# —————————————————————————————
# CONFIG
# —————————————————————————————
PER_POST_SLEEP = 1.0
ITER_THROTTLE_SEC = 0.75  # tiny delay per post to reduce GraphQL pressure

MEDIA_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov"}
META_EXTS = {".txt", ".json", ".xz", ".xml", ".log"}

BACKOFF_BASE_SEC = 120
BACKOFF_CAP_SEC = 480
MAX_RETRIES_PROFILE = 6
MAX_RETRIES_POST = 2

# Daily-mode early stop tuning
CONSEC_SEEN_STOP = 8  # streak of already-seen before we bail when nothing new

# Date filters (set via CLI)
DATE_AFTER_UTC: Optional[datetime] = None
DATE_BEFORE_UTC: Optional[datetime] = None

LOG_DIR = os.path.join(SCRIPT_DIR, "log")
os.makedirs(LOG_DIR, exist_ok=True)
RUN_TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
RUN_LOG = os.path.join(LOG_DIR, f"insta_download_unified_{RUN_TS}.txt")

# —————————————————————————————
# Logging
# —————————————————————————————

def _log_line(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(RUN_LOG, "a", encoding="utf-8") as fh:
            fh.write(f"{ts} | {msg}\n")
    except Exception:
        pass

# —————————————————————————————
# Housekeeping
# —————————————————————————————

def is_temp(fname: str) -> bool:
    return fname.endswith(".tmp") or fname.endswith(".part") or fname.endswith("~")


def initial_cleanup():
    # Fix accidental "downloads﹨profile" folders dragged from Windows UI
    for item in os.listdir("."):
        if os.path.isdir(item) and "﹨" in item:
            parts = item.split("﹨", 1)
            if parts[0] == "downloads":
                profile = parts[1]
                os.makedirs("downloads", exist_ok=True)
                src = item
                dst = os.path.join("downloads", profile)
                if not os.path.exists(dst):
                    try:
                        shutil.move(src, dst)
                    except Exception:
                        pass

    # Fix nested weird names inside downloads/
    if os.path.exists("downloads"):
        for folder in os.listdir("downloads"):
            if "﹨" in folder:
                parts = folder.split("﹨", 1)
                profile = parts[1]
                wrong = os.path.join("downloads", folder)
                correct = os.path.join("downloads", profile)
                if not os.path.exists(correct):
                    try:
                        shutil.move(wrong, correct)
                    except Exception:
                        pass


def ensure_dirs_for_profile(base_path: str):
    media_path = os.path.join(base_path, "media")
    meta_path = os.path.join(base_path, "metadata")
    os.makedirs(media_path, exist_ok=True)
    os.makedirs(meta_path, exist_ok=True)
    return media_path, meta_path


def move_sorted(base_path: str):
    media_path = os.path.join(base_path, "media")
    meta_path = os.path.join(base_path, "metadata")
    os.makedirs(media_path, exist_ok=True)
    os.makedirs(meta_path, exist_ok=True)
    for fname in os.listdir(base_path):
        full = os.path.join(base_path, fname)
        if os.path.isdir(full) or is_temp(fname):
            continue
        ext = os.path.splitext(fname)[1].lower()
        try:
            if ext in MEDIA_EXTS:
                shutil.move(full, os.path.join(media_path, fname))
            elif ext in META_EXTS:
                shutil.move(full, os.path.join(meta_path, fname))
        except shutil.Error:
            pass

# —————————————————————————————
# Filenames / identity
# —————————————————————————————

def expected_basename_from_post(post) -> str:
    dt = getattr(post, "date_utc", None)
    if dt is None:
        return ""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    else:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d_%H-%M-%S_UTC")


def get_post_identifiers(post):
    shortcode = getattr(post, "shortcode", None) or "unknown"
    basename = expected_basename_from_post(post) or shortcode
    ts_iso = getattr(post, "date_utc", None)
    if ts_iso is not None:
        if ts_iso.tzinfo is None:
            ts_iso = ts_iso.replace(tzinfo=timezone.utc)
        else:
            ts_iso = ts_iso.astimezone(timezone.utc)
        ts_iso = ts_iso.isoformat()
    else:
        ts_iso = ""
    return basename, shortcode, ts_iso

# —————————————————————————————
# Disk existence checks (multi-dir)
# —————————————————————————————

def _multi_dir_iter(dir_paths):
    if isinstance(dir_paths, str):
        dir_paths = [dir_paths]
    for d in dir_paths:
        yield d


def any_mp4_exists_for_post(dir_paths, shortcode, basename) -> bool:
    from re import compile, escape, IGNORECASE
    sc_pat = compile(rf"^{escape(shortcode)}(?:_.+)?\.mp4$", IGNORECASE)
    bs_pat = compile(rf"^{escape(basename)}(?:_.+)?\.mp4$", IGNORECASE)
    for d in _multi_dir_iter(dir_paths):
        try:
            for fn in os.listdir(d):
                if sc_pat.match(fn) or bs_pat.match(fn):
                    return True
        except FileNotFoundError:
            pass
    return False


def any_media_exists_for_post(dir_paths, shortcode, basename) -> bool:
    from re import compile, escape, IGNORECASE
    exts = r"(?:jpg|jpeg|png|webp|mp4|mov)"
    pat1 = compile(rf"^{escape(basename)}(?:_.+)?\.{exts}$", IGNORECASE)
    pat2 = compile(rf"^{escape(shortcode)}(?:_.+)?\.{exts}$", IGNORECASE)
    for d in _multi_dir_iter(dir_paths):
        try:
            for fn in os.listdir(d):
                if pat1.match(fn) or pat2.match(fn):
                    return True
        except FileNotFoundError:
            pass
    return False

# —————————————————————————————
# Log guard wrapper + backfill
# —————————————————————————————

def already_logged_wrapper(post, log_dir) -> bool:
    try:
        basename, shortcode, _ = get_post_identifiers(post)
        return already_logged_post(log_dir, basename, shortcode)
    except TypeError:
        basename, shortcode, _ = get_post_identifiers(post)
        try:
            return already_logged_post(basename, shortcode)
        except Exception as e:
            print(f"   (log_guard warning) {e}")
            return False
    except Exception as e:
        print(f"   (log_guard warning) {e}")
        return False


def backfill_log_from_disk(media_dir: str):
    if not os.path.isdir(media_dir):
        return
    pat = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_UTC)(?:_.+)?\.(?:jpg|jpeg|png|webp|mp4|mov)$", re.I)
    for fn in os.listdir(media_dir):
        m = pat.match(fn)
        if not m:
            continue
        basename = m.group(1)
        shortcode = None
        meta_json = os.path.join(os.path.dirname(media_dir), "metadata", f"{basename}.json")
        if os.path.exists(meta_json):
            try:
                with open(meta_json, "r", encoding="utf-8") as fh:
                    j = json.load(fh)
                    shortcode = j.get("shortcode") or j.get("node", {}).get("shortcode")
            except Exception:
                pass
        shortcode = shortcode or "unknown"
        try:
            already_logged_post(media_dir, basename, shortcode)
        except Exception:
            pass

# —————————————————————————————
# Marker helpers (daily boundary stop)
# —————————————————————————————

def _marker_path(meta_dir: str, stream: str) -> str:
    return os.path.join(meta_dir, f"latest_seen_{stream}.json")


def load_marker(meta_dir: str, stream: str) -> Optional[Tuple[str, str]]:
    path = _marker_path(meta_dir, stream)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            j = json.load(fh)
        ts = j.get("ts")
        sc = j.get("shortcode")
        if not ts:
            return None
        return (ts, sc or "")
    except Exception:
        return None


def save_marker(meta_dir: str, stream: str, ts_iso: str, shortcode: str):
    path = _marker_path(meta_dir, stream)
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"ts": ts_iso, "shortcode": shortcode or ""}, fh, ensure_ascii=False)
    except Exception:
        pass


def ident_tuple(ts_iso: str, shortcode: str) -> Tuple[str, str]:
    return (ts_iso or "", shortcode or "")

def debug_save_marker(meta_dir: str, stream: str, ts_iso: str, shortcode: str, reason: str):
    save_marker(meta_dir, stream, ts_iso, shortcode)
    print(f"   🏷️ wrote marker latest_seen_{stream}.json → ts={ts_iso} sc={shortcode} ({reason})")

def _find_newest_from_disk(media_dir: str) -> Optional[Tuple[str, str]]:
    """Look into metadata JSON files to find newest ts/shortcode already downloaded."""
    meta_dir = os.path.join(os.path.dirname(media_dir), "metadata")
    if not os.path.isdir(meta_dir):
        return None
    newest = None
    for fn in os.listdir(meta_dir):
        if not fn.lower().endswith(".json"):
            continue
        path = os.path.join(meta_dir, fn)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                j = json.load(fh)
            ts = j.get("date_utc") or j.get("taken_at") or j.get("date")
            sc = j.get("shortcode") or j.get("node", {}).get("shortcode") or ""
            if not ts:
                m = re.match(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_UTC", os.path.splitext(fn)[0])
                if m:
                    dt = datetime.strptime(m.group(1), "%Y-%m-%d_%H-%M-%S").replace(tzinfo=timezone.utc)
                    ts = dt.isoformat()
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            except Exception:
                try:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    continue
            iso = dt.astimezone(timezone.utc).isoformat()
            ident = (iso, sc or "")
            if newest is None or ident > newest:
                newest = ident
        except Exception:
            continue
    return newest

# —————————————————————————————
# Fallback video rescue
# —————————————————————————————

def stream_save(session, url, dest_path):
    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"   ↪  Fallback download failed: {e}")
        return False


def ensure_post_videos(L, post, media_dir, base_dir):
    rescued = 0
    session = L.context._session
    basename, shortcode, _ = get_post_identifiers(post)

    def mp4_exists():
        return any_mp4_exists_for_post([base_dir, media_dir], shortcode, basename)

    # Main video / reel
    if getattr(post, "is_video", False):
        if not mp4_exists() and getattr(post, "video_url", None):
            dest = os.path.join(media_dir, f"{basename}.mp4")
            print(f"   ↪  No mp4 for {shortcode}. Fallback → {os.path.basename(dest)}")
            if stream_save(session, post.video_url, dest):
                rescued += 1

    # Sidecar videos (check specific files only)
    try:
        if hasattr(post, "get_sidecar_nodes"):
            nodes = list(post.get_sidecar_nodes())
            for i, node in enumerate(nodes):
                if getattr(node, "is_video", False) and getattr(node, "video_url", None):
                    side_name = f"{basename}_{i+1}.mp4"
                    side_in_media = os.path.join(media_dir, side_name)
                    side_in_base  = os.path.join(base_dir,  side_name)
                    if not (os.path.exists(side_in_media) or os.path.exists(side_in_base)):
                        print(f"   ↪  Sidecar missing. Fallback for {shortcode} [{i+1}] → {side_name}")
                        if stream_save(session, node.video_url, side_in_media):
                            rescued += 1
    except Exception as e:
        print(f"   ↪  Sidecar probe failed: {e}")

    return rescued

# —————————————————————————————
# Sessions & rotation
# —————————————————————————————

HARD_ROTATE_ERRORS = (
    exceptions.LoginRequiredException,
    exceptions.TwoFactorAuthRequiredException,
    exceptions.ConnectionException,
    getattr(exceptions, "TooManyRequestsException", exceptions.ConnectionException),
    getattr(exceptions, "QueryReturnedForbiddenException", exceptions.ConnectionException),
    getattr(exceptions, "BadResponseException", exceptions.ConnectionException),
)

def _parse_cookie_kv(s: str) -> Dict[str, str]:
    out = {}
    parts = [p.strip() for p in s.split(";") if p.strip()]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def _parse_sessions_line(line: str) -> Dict[str, Optional[str]]:
    line = line.strip()
    if not line:
        return {}
    if "sessionid=" in line:
        kv = _parse_cookie_kv(line)
        return {
            "sessionid": kv.get("sessionid"),
            "ds_user_id": kv.get("ds_user_id"),
            "csrftoken": kv.get("csrftoken"),
            "username": kv.get("ds_user") or kv.get("username")
        }
    for sep in ["|", ","]:
        if sep in line:
            u, sid = [x.strip() for x in line.split(sep, 1)]
            return {"sessionid": sid, "ds_user_id": None, "csrftoken": None, "username": u}
    parts = line.split()
    if len(parts) == 2:
        return {"sessionid": parts[1].strip(), "ds_user_id": None, "csrftoken": None, "username": parts[0].strip()}
    return {"sessionid": line, "ds_user_id": None, "csrftoken": None, "username": None}

def load_sessions() -> List[Dict[str, Optional[str]]]:
    sessions_file = os.path.join(SCRIPT_DIR, "sessions.txt")
    if not os.path.exists(sessions_file):
        raise FileNotFoundError(f"No sessions.txt file found at: {sessions_file}")
    entries = []
    with open(sessions_file, "r", encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            info = _parse_sessions_line(ln)
            if info.get("sessionid"):
                entries.append(info)
    if not entries:
        raise RuntimeError("sessions.txt is empty or no valid sessionid found.")
    return entries

def _session_label(entry: Dict[str, Optional[str]]) -> str:
    u = entry.get("username")
    sid = entry.get("sessionid", "")
    if u:
        return u
    if len(sid) > 6:
        return f"{sid[:3]}…{sid[-3:]}"
    return sid or "unknown"

def make_loader(entry: Dict[str, Optional[str]]) -> instaloader.Instaloader:
    L = instaloader.Instaloader(
        download_pictures=True,
        download_videos=True,
        download_video_thumbnails=False,
        save_metadata=True,
        post_metadata_txt_pattern=None,   # avoid .txt sidecars
        compress_json=False,              # save plain .json (no .json.xz)
        max_connection_attempts=3
    )
    s = L.context._session
    try:
        s.get("https://www.instagram.com/", timeout=30)
    except Exception:
        pass
    if entry.get("sessionid"):
        s.cookies.set("sessionid", entry["sessionid"], domain=".instagram.com")
    if entry.get("ds_user_id"):
        s.cookies.set("ds_user_id", entry["ds_user_id"], domain=".instagram.com")
    if entry.get("csrftoken"):
        s.cookies.set("csrftoken", entry["csrftoken"], domain=".instagram.com")
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Referer": "https://www.instagram.com/"
    })
    return L

class SessionManager:
    """Manage current loader and rotate by time and on hard errors."""
    def __init__(self, sessions: List[Dict[str, Optional[str]]], rotate_interval_sec: int = 120):
        self.sessions = sessions
        self.rotate_interval = max(10, int(rotate_interval_sec))
        self.idx = 0
        self.L = make_loader(self.sessions[self.idx])
        self.last_rotate = time.time()
        print(f"🔐 Using session: {_session_label(self.sessions[self.idx])}")

    def maybe_time_rotate(self):
        now = time.time()
        if now - self.last_rotate >= self.rotate_interval and len(self.sessions) > 1:
            old = self.idx
            self.idx = (self.idx + 1) % len(self.sessions)
            self.L = make_loader(self.sessions[self.idx])
            self.last_rotate = now
            print(f"   🔄 Time-rotate session: {_session_label(self.sessions[old])} → {_session_label(self.sessions[self.idx])}")
            _log_line(f"TIME_ROTATE {_session_label(self.sessions[old])} -> {_session_label(self.sessions[self.idx])}")

    def rotate_on_error(self) -> bool:
        if len(self.sessions) <= 1:
            return False
        old = self.idx
        self.idx = (self.idx + 1) % len(self.sessions)
        self.L = make_loader(self.sessions[self.idx])
        self.last_rotate = time.time()
        print(f"   🔄 Error-rotate session: {_session_label(self.sessions[old])} → {_session_label(self.sessions[self.idx])}")
        _log_line(f"ERROR_ROTATE {_session_label(self.sessions[old])} -> {_session_label(self.sessions[self.idx])}")
        return True

# —————————————————————————————
# CLI parsing
# —————————————————————————————

def _parse_dt_utc(s: str) -> datetime:
    s = s.strip()
    fmts = ["%Y-%m-%d", "%Y-%m-%d %H:%M"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Invalid date/time format: '{s}'. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'.")

def parse_cli():
    p = argparse.ArgumentParser(description="Unified Instagram downloader with timed session rotation + marker-based daily stop.")
    p.add_argument("mode", choices=["daily", "init", "all"], help="daily = fast/stop-early; init = bulk history; all = daily + stories/highlights")
    p.add_argument("profiles", nargs="*", help="Profile usernames (space or comma-separated).")
    p.add_argument("-f", dest="file", help="File with one profile per line.")
    p.add_argument("--after", dest="after", help="Only posts strictly AFTER this (YYYY-MM-DD or 'YYYY-MM-DD HH:MM').")
    p.add_argument("--before", dest="before", help="Only posts strictly BEFORE this (YYYY-MM-DD or 'YYYY-MM-DD HH:MM').")
    p.add_argument("--feed-only", action="store_true")
    p.add_argument("--reels-only", action="store_true")
    p.add_argument("--stories-only", action="store_true")
    p.add_argument("--highlights-only", action="store_true")
    p.add_argument("--rotate-interval", type=int, default=120, help="Seconds between automatic session rotations. Default 120s.")
    return p.parse_args()

def parse_profiles_from_cli(args) -> List[str]:
    items = []
    if args.profiles:
        for tok in args.profiles:
            if "," in tok:
                items.extend([p.strip() for p in tok.split(",") if p.strip()])
            else:
                tok = tok.strip()
                if tok:
                    items.append(tok)
    if args.file:
        if not os.path.exists(args.file):
            raise FileNotFoundError(f"Profiles file not found: {args.file}")
        with open(args.file, "r", encoding="utf-8") as fh:
            items.extend([ln.strip() for ln in fh if ln.strip() and not ln.strip().startswith("#")])
    if not items:
        profiles_file = os.path.join(SCRIPT_DIR, "profiles.txt")
        if not os.path.exists(profiles_file):
            raise FileNotFoundError(
                f"No profiles provided and {profiles_file} not found.\n"
                f"Usage: python {os.path.basename(__file__)} MODE profile1 [profile2] [--after ...] [--before ...]"
            )
        with open(profiles_file, "r", encoding="utf-8") as fh:
            items.extend([ln.strip() for ln in fh if ln.strip() and not ln.strip().startswith("#")])
    seen = set(); out = []
    for p in items:
        if p not in seen:
            out.append(p); seen.add(p)
    return out

# —————————————————————————————
# Date filter + throttles
# —————————————————————————————

def _safe_sleep_backoff(attempt: int):
    delay = min(BACKOFF_BASE_SEC * (2 ** max(0, attempt - 1)), BACKOFF_CAP_SEC)
    print(f"   ⏳ Backoff {delay}s (attempt {attempt}) …")
    time.sleep(delay)

def _post_passes_date_filter(post) -> bool:
    if DATE_AFTER_UTC is None and DATE_BEFORE_UTC is None:
        return True
    pdt = getattr(post, "date_utc", None)
    if pdt is None:
        return True
    if pdt.tzinfo is None:
        pdt = pdt.replace(tzinfo=timezone.utc)
    else:
        pdt = pdt.astimezone(timezone.utc)
    if DATE_AFTER_UTC and pdt <= DATE_AFTER_UTC:
        return False
    if DATE_BEFORE_UTC and pdt >= DATE_BEFORE_UTC:
        return False
    return True

# —————————————————————————————
# Download helpers
# —————————————————————————————

def _download_one_post(sman: "SessionManager", post, base_path: str) -> Tuple[bool, int]:
    sman.maybe_time_rotate()
    L = sman.L
    media_dir, _ = ensure_dirs_for_profile(base_path)
    rescued = 0

    def try_fallback_video():
        nonlocal rescued
        basename, shortcode, _ = get_post_identifiers(post)
        if getattr(post, "is_video", False) and getattr(post, "video_url", None):
            if not any_mp4_exists_for_post([base_path, media_dir], shortcode, basename):
                dest = os.path.join(media_dir, f"{basename}.mp4")
                print(f"   ↪️  Direct video fallback → {os.path.basename(dest)}")
                if stream_save(L.context._session, post.video_url, dest):
                    rescued += 1
                    return True
        try:
            if hasattr(post, "get_sidecar_nodes"):
                nodes = list(post.get_sidecar_nodes())
                for i, node in enumerate(nodes):
                    if getattr(node, "is_video", False) and getattr(node, "video_url", None):
                        side_name = f"{basename}_{i+1}.mp4"
                        side_in_media = os.path.join(media_dir, side_name)
                        side_in_base  = os.path.join(base_path,  side_name)
                        if not (os.path.exists(side_in_media) or os.path.exists(side_in_base)):
                            print(f"   ↪️  Direct sidecar fallback [{i+1}] → {side_name}")
                            if stream_save(L.context._session, node.video_url, side_in_media):
                                rescued += 1
        except Exception:
            pass
        return False

    for attempt in range(1, MAX_RETRIES_POST + 1):
        try:
            L.download_post(post, target=base_path)
            rescued += ensure_post_videos(L, post, media_dir, base_path)
            move_sorted(base_path)
            return True, rescued

        except HARD_ROTATE_ERRORS as e:
            if "500 Internal Server Error" in str(e) or "BadResponse" in type(e).__name__:
                if try_fallback_video():
                    move_sorted(base_path)
                    return True, rescued
            print(f"   🚧 Post error: {e}")
            _log_line(f"POST_ERROR {type(e).__name__}: {e}")
            if sman.rotate_on_error():
                _safe_sleep_backoff(min(attempt, 3))
                continue
            if attempt < MAX_RETRIES_POST:
                _safe_sleep_backoff(attempt)
                continue
            return False, rescued

        except Exception as e:
            if "500 Internal Server Error" in str(e) or "Internal Server Error" in str(e):
                if try_fallback_video():
                    move_sorted(base_path)
                    return True, rescued
            print(f"   ❗ Unexpected post error: {e}")
            if attempt < MAX_RETRIES_POST:
                _safe_sleep_backoff(attempt)
                continue
            return False, rescued

# —————————————————————————————
# Marker-driven iterator (daily & init)
# —————————————————————————————

def download_posts_iter(kind: str, iterable: Iterable, sman: "SessionManager", base_path: str) -> Tuple[int, int]:
    """Unified iterator with marker-based boundary stop in daily mode.
       kind: "feed" or "reels" (others ignored)
    """
    count = 0
    rescued_total = 0

    media_dir, meta_dir = ensure_dirs_for_profile(base_path)

    stream = "feed" if kind == "feed" else ("reels" if kind == "reels" else kind)
    marker = load_marker(meta_dir, stream)
    max_seen: Optional[Tuple[str, str]] = marker  # (ts_iso, shortcode)
    top_ident_seen: Optional[Tuple[str, str]] = None

    downloaded_this_run = 0
    consec_seen = 0

    for post in iterable:  # newest → oldest
        time.sleep(ITER_THROTTLE_SEC)
        sman.maybe_time_rotate()

        if not _post_passes_date_filter(post):
            continue

        basename, shortcode, ts_iso = get_post_identifiers(post)
        ident = ident_tuple(ts_iso, shortcode)
        if top_ident_seen is None or ident > top_ident_seen:
            top_ident_seen = ident  # remember the newest we *saw* during enumeration

        # Marker-based boundary logic only in daily mode
        is_daily = CURRENT_MODE == "daily"
        if is_daily and marker is not None:
            if ident <= marker:
                if downloaded_this_run > 0:
                    break  # crossed into old territory after new stuff—stop
                consec_seen += 1
                if consec_seen >= CONSEC_SEEN_STOP:
                    break
            else:
                consec_seen = 0  # encountering newer content resets streak

        # Log/disk guards (fast skip)
        if already_logged_wrapper(post, media_dir):
            if is_daily and downloaded_this_run == 0:
                consec_seen += 1
                if consec_seen >= CONSEC_SEEN_STOP:
                    break
            continue

        if any_media_exists_for_post([base_path, media_dir], shortcode, basename):
            if is_daily and downloaded_this_run == 0:
                consec_seen += 1
                if consec_seen >= CONSEC_SEEN_STOP:
                    break
            continue

        ok, rescued = _download_one_post(sman, post, base_path)
        if ok:
            count += 1
            rescued_total += rescued
            downloaded_this_run += 1
            consec_seen = 0
            if max_seen is None or ident > max_seen:
                max_seen = ident
            time.sleep(PER_POST_SLEEP)

    # Persist marker:
    # - If we advanced via actual downloads, persist max_seen.
    # - If there was no marker and we enumerated but downloaded nothing, seed from top_ident_seen.
    # - Else (blocked or empty), let caller decide (process_profile may seed from disk).
    if CURRENT_MODE in ("daily", "all"):
        if max_seen is not None and (marker is None or max_seen > marker) and downloaded_this_run > 0:
            debug_save_marker(meta_dir, stream, max_seen[0], max_seen[1], "advanced via downloads")
        elif marker is None and top_ident_seen is not None and downloaded_this_run == 0:
            debug_save_marker(meta_dir, stream, top_ident_seen[0], top_ident_seen[1], "seed from enumeration (no downloads)")

    return count, rescued_total

# —————————————————————————————
# Stories / Highlights
# —————————————————————————————

def download_stories(profile: instaloader.Profile, sman: "SessionManager", base_path: str) -> int:
    count = 0
    ensure_dirs_for_profile(base_path)
    for attempt in range(1, MAX_RETRIES_POST + 1):
        try:
            sman.maybe_time_rotate()
            L = sman.L
            for story in L.get_stories(userids=[profile.userid]):
                for item in story.get_items():
                    for a2 in range(1, MAX_RETRIES_POST + 1):
                        try:
                            sman.maybe_time_rotate()
                            L.download_storyitem(item, target=os.path.join(base_path, "stories"))
                            count += 1
                            move_sorted(base_path)
                            break
                        except HARD_ROTATE_ERRORS as e:
                            if sman.rotate_on_error():
                                _safe_sleep_backoff(min(a2, 3))
                                continue
                            if a2 < MAX_RETRIES_POST:
                                _safe_sleep_backoff(a2)
                                continue
                            break
                        except Exception:
                            break
            break
        except HARD_ROTATE_ERRORS as e:
            if sman.rotate_on_error():
                _safe_sleep_backoff(min(attempt, 3))
                continue
            if attempt < MAX_RETRIES_POST:
                _safe_sleep_backoff(attempt)
                continue
            break
        except Exception:
            break
    return count

def _sanitize_title(s: str) -> str:
    s = s or ""
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s or "untitled"

def download_highlights(profile: instaloader.Profile, sman: "SessionManager", base_path: str) -> int:
    count = 0
    try:
        sman.maybe_time_rotate()
        L = sman.L
        for hl in profile.get_highlights():
            title = _sanitize_title(getattr(hl, "title", "") or f"highlight_{hl.unique_id}")
            target_dir = os.path.join(base_path, "highlights", title)
            os.makedirs(target_dir, exist_ok=True)
            for item in hl.get_items():
                for attempt in range(1, MAX_RETRIES_POST + 1):
                    try:
                        sman.maybe_time_rotate()
                        L.download_storyitem(item, target=target_dir)
                        count += 1
                        move_sorted(base_path)
                        break
                    except HARD_ROTATE_ERRORS as e:
                        if sman.rotate_on_error():
                            _safe_sleep_backoff(min(attempt, 3))
                            continue
                        if attempt < MAX_RETRIES_POST:
                            _safe_sleep_backoff(attempt)
                            continue
                        else:
                            break
                    except Exception:
                        break
    except Exception:
        pass
    return count

# —————————————————————————————
# Probes (bounded retries; no infinite 403 spam)
# —————————————————————————————

def _probe_iter(it):
    # Pull just one item to ensure the iterator is alive.
    try:
        next(it)
        return True
    except StopIteration:
        return True  # empty but not failing
    except Exception:
        return False

def _bounded_probe(name: str, it_factory, sman: "SessionManager", tries: int = 4) -> bool:
    """Try to touch an iterator with bounded retries and rotation."""
    for i in range(1, tries + 1):
        sman.maybe_time_rotate()
        try:
            it = it_factory()
            ok = _probe_iter(it)
            if ok:
                return True
        except (exceptions.TooManyRequestsException,
                getattr(exceptions, "QueryReturnedForbiddenException", exceptions.ConnectionException),
                exceptions.BadResponseException,
                exceptions.ConnectionException) as e:
            print(f"   🚧 {name} probe 403/err: {e} | try {i}/{tries}")
            _log_line(f"{name.upper()}_PROBE_ERROR {type(e).__name__}: {e}")
            if sman.rotate_on_error():
                _safe_sleep_backoff(min(i, 3))
                continue
            _safe_sleep_backoff(i)
        except Exception as e:
            print(f"   ❗ {name} probe unexpected: {e} | try {i}/{tries}")
            _safe_sleep_backoff(i)
    print(f"   ⚠️ {name} probe failed repeatedly (403/blocked). Skipping {name.upper()} for this profile.")
    return False

# —————————————————————————————
# Profile processing
# —————————————————————————————

def process_profile(sman: "SessionManager", profile_name: str, mode: str, base_path: str,
                    feed_only: bool, reels_only: bool, stories_only: bool, highlights_only: bool):
    attempts = 0
    feed_count = reels_count = stories_count = highlights_count = rescued_count = 0

    while True:
        attempts += 1
        try:
            sman.maybe_time_rotate()
            p = instaloader.Profile.from_username(sman.L.context, profile_name)

            media_dir, meta_dir = ensure_dirs_for_profile(base_path)
            backfill_log_from_disk(media_dir)

            # FEED
            if not reels_only and not stories_only and not highlights_only:
                print("📌 Downloading FEED posts…")
                probe_ok = _bounded_probe("feed", lambda: p.get_posts(), sman)
                if probe_ok:
                    fc, r = download_posts_iter("feed", p.get_posts(), sman, base_path)
                    feed_count += fc; rescued_count += r
                else:
                    # Seed marker from disk if none exists
                    if load_marker(meta_dir, "feed") is None:
                        disk_top = _find_newest_from_disk(media_dir)
                        if disk_top:
                            debug_save_marker(meta_dir, "feed", disk_top[0], disk_top[1], "seed from disk (probe blocked)")

            # REELS
            if (mode in ("daily", "init") and not feed_only) or reels_only:
                if hasattr(p, "get_reels"):
                    print("📌 Downloading REELS…")
                    probe_ok = _bounded_probe("reels", lambda: p.get_reels(), sman)
                    if probe_ok:
                        rc, r = download_posts_iter("reels", p.get_reels(), sman, base_path)
                        reels_count += rc; rescued_count += r
                    else:
                        if load_marker(meta_dir, "reels") is None:
                            disk_top = _find_newest_from_disk(media_dir)
                            if disk_top:
                                debug_save_marker(meta_dir, "reels", disk_top[0], disk_top[1], "seed from disk (probe blocked)")

            # STORIES
            if mode in ("all",) or stories_only:
                print("📌 Downloading STORIES…")
                stories_count += download_stories(p, sman, base_path)

            # HIGHLIGHTS
            if mode in ("all",) or highlights_only:
                print("📌 Downloading HIGHLIGHTS…")
                highlights_count += download_highlights(p, sman, base_path)

            move_sorted(base_path)
            return feed_count, reels_count, stories_count, highlights_count, rescued_count

        except HARD_ROTATE_ERRORS as e:
            print(f"   🚧 Hard error on profile '{profile_name}': {e}")
            _log_line(f"profile={profile_name} | HARD_ERROR={type(e).__name__}: {e}")
            if sman.rotate_on_error() and attempts < MAX_RETRIES_PROFILE:
                _safe_sleep_backoff(min(attempts, 3))
                continue
            print("   ❌ Could not recover via rotation/retries for this profile.")
            return feed_count, reels_count, stories_count, highlights_count, rescued_count
        except exceptions.PrivateProfileNotFollowedException:
            print(f"   🔒 Private profile (not followed): {profile_name}. Skipping.")
            return feed_count, reels_count, stories_count, highlights_count, rescued_count
        except Exception as e:
            print(f"   ❗ Error in profile '{profile_name}': {e}")
            _log_line(f"profile={profile_name} | ERROR={type(e).__name__}: {e}")
            return feed_count, reels_count, stories_count, highlights_count, rescued_count

# —————————————————————————————
# Main
# —————————————————————————————

CURRENT_MODE = "daily"  # set during main()

def main():
    global CURRENT_MODE, DATE_AFTER_UTC, DATE_BEFORE_UTC

    initial_cleanup()
    os.makedirs("downloads", exist_ok=True)

    args = parse_cli()
    CURRENT_MODE = args.mode

    if args.after:
        DATE_AFTER_UTC = _parse_dt_utc(args.after)
    if args.before:
        DATE_BEFORE_UTC = _parse_dt_utc(args.before)

    sessions = load_sessions()
    sman = SessionManager(sessions, rotate_interval_sec=args.rotate_interval)

    profiles = parse_profiles_from_cli(args)

    _log_line("=== RUN START ===")
    _log_line(f"mode={args.mode}")
    _log_line(f"profiles={profiles}")
    if DATE_AFTER_UTC:
        _log_line(f"DATE_AFTER_UTC={DATE_AFTER_UTC.isoformat()}")
    if DATE_BEFORE_UTC:
        _log_line(f"DATE_BEFORE_UTC={DATE_BEFORE_UTC.isoformat()}")
    _log_line(f"rotate_interval={args.rotate_interval}s")

    total_feed = total_reels = total_stories = total_highlights = total_rescued = 0

    for profile in profiles:
        print(f"\n📥 Processing: {profile} [{args.mode}]")
        if DATE_AFTER_UTC or DATE_BEFORE_UTC:
            a = DATE_AFTER_UTC.strftime('%Y-%m-%d %H:%M UTC') if DATE_AFTER_UTC else ''
            b = DATE_BEFORE_UTC.strftime('%Y-%m-%d %H:%M UTC') if DATE_BEFORE_UTC else ''
            print(f"   📅 Filter: after={a} before={b}")
        base_path = os.path.join("downloads", profile)
        os.makedirs(base_path, exist_ok=True)
        ensure_dirs_for_profile(base_path)

        fc, rc, sc, hc, rsc = process_profile(
            sman,
            profile,
            args.mode,
            base_path,
            feed_only=args.feed_only,
            reels_only=args.reels_only,
            stories_only=args.stories_only,
            highlights_only=args.highlights_only,
        )

        _log_line(
            f"profile={profile} | feed={fc} | reels={rc} | stories={sc} | highlights={hc} | rescued={rsc}"
        )

        total_feed += fc
        total_reels += rc
        total_stories += sc
        total_highlights += hc
        total_rescued += rsc

    _log_line(
        f"=== RUN END === total_profiles={len(profiles)}, feed={total_feed}, reels={total_reels}, "
        f"stories={total_stories}, highlights={total_highlights}, rescued_videos={total_rescued}"
    )
    print("\n✅ All profiles processed.")

if __name__ == "__main__":
    main()
