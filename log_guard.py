# log_guard.py — reads per-profile logs from /srv/igdl/media_log/{profile}_media_log.csv
import csv
import os
from typing import Set

MEDIA_LOG_ROOT = "/srv/igdl/media_log"          # central location for all CSVs
FALLBACK_NAME  = "media_log.csv"                # fallback inside each profile's media/ if needed

def _infer_profile_from_media_dir(media_dir: str) -> str:
    """
    media_dir is usually .../downloads/<profile>/media
    We take the parent directory name of 'media' as the profile.
    """
    try:
        return os.path.basename(os.path.dirname(os.path.abspath(media_dir)))
    except Exception:
        return ""

def _load_logged_filenames(media_dir: str) -> Set[str]:
    """
    Prefer /srv/igdl/media_log/<profile>_media_log.csv.
    If not present, fall back to <media_dir>/media_log.csv.
    Returns a set of filenames (strings).
    """
    names: Set[str] = set()

    # 1) Try central CSV: /srv/igdl/media_log/<profile>_media_log.csv
    profile = _infer_profile_from_media_dir(media_dir)
    if profile:
        central_path = os.path.join(MEDIA_LOG_ROOT, f"{profile}_media_log.csv")
        if os.path.isfile(central_path):
            _read_csv_into_set(central_path, names)

    # 2) Fallback: local CSV inside the media folder
    if not names:
        local_path = os.path.join(media_dir, FALLBACK_NAME)
        if os.path.isfile(local_path):
            _read_csv_into_set(local_path, names)

    return names

def _read_csv_into_set(path: str, out: Set[str]) -> None:
    """
    Reads a CSV and adds the 'filename' column to 'out'.
    Accepts headers with different casings or extra columns.
    """
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            field_map = {k.lower(): k for k in (reader.fieldnames or [])}
            fname_col = field_map.get("filename")
            if not fname_col:
                return
            for row in reader:
                fn = (row.get(fname_col) or "").strip()
                if fn:
                    out.add(fn)
    except Exception:
        pass

def already_logged_by_basename(media_dir: str, basename: str) -> bool:
    basename = (basename or "").strip()
    if not basename:
        return False
    for name in _load_logged_filenames(media_dir):
        if name.startswith(basename):
            return True
    return False

def already_logged_by_shortcode(media_dir: str, shortcode: str) -> bool:
    shortcode = (shortcode or "").strip()
    if not shortcode:
        return False
    token = f"{shortcode}"
    for name in _load_logged_filenames(media_dir):
        if token in name:
            return True
    return False

def already_logged_post(media_dir: str, basename: str, shortcode: str) -> bool:
    return already_logged_by_basename(media_dir, basename) or \
           already_logged_by_shortcode(media_dir, shortcode)
