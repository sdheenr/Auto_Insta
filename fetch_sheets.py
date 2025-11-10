import os, csv, io, requests

ROOT = "/srv/igdl"
ENV = {}

# read .env
env_path = os.path.join(ROOT, ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            ENV[k.strip()] = v.strip()

PROFILES_URL = ENV.get("GSHEET_PROFILES_CSV", "")
SESSIONS_URL = ENV.get("GSHEET_SESSIONS_CSV", "")

def _fetch_csv_text(url: str) -> str:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def _parse_profiles(text: str) -> list[str]:
    out = []
    f = io.StringIO(text)
    rows = list(csv.reader(f))
    if not rows:
        return out

    # If header-like first row contains a known field, use DictReader
    hdr = [c.strip().lower() for c in rows[0]]
    if any(x in hdr for x in ("profile", "profiles", "username")):
        f.seek(0)
        reader = csv.DictReader(f)
        fields = {k.lower(): k for k in (reader.fieldnames or [])}
        pick = fields.get("profile") or fields.get("profiles") or fields.get("username")
        if pick:
            for row in reader:
                val = (row.get(pick) or "").strip()
                if val and not val.startswith("#"):
                    out.append(val)
            return out

    # fallback: first column
    for row in rows:
        if not row:
            continue
        val = (row[0] or "").strip()
        if val and not val.startswith("#"):
            out.append(val)
    return out

def _parse_sessions(text: str) -> list[str]:
    """
    Supported layouts:
      A) header row with username,sessionid  -> emit "username|sessionid"
      B) header row with cookie/cookies      -> emit cookie string as-is
      C) single column (raw sessionid/cookie per line)
    """
    out = []
    f = io.StringIO(text)
    rows = list(csv.reader(f))
    if not rows:
        return out

    hdr = [c.strip().lower() for c in rows[0]]

    # B) cookie/cookies column present
    if ("cookie" in hdr) or ("cookies" in hdr):
        f.seek(0)
        reader = csv.DictReader(f)
        fields = {k.lower(): k for k in (reader.fieldnames or [])}
        ck = fields.get("cookie") or fields.get("cookies")
        for row in reader:
            s = (row.get(ck) or "").strip()
            if s and not s.startswith("#"):
                out.append(s)
        return out

    # A) username + sessionid columns present
    if ("username" in hdr) and ("sessionid" in hdr):
        user_idx = hdr.index("username")
        sid_idx  = hdr.index("sessionid")
        for row in rows[1:]:
            if not row:
                continue
            u = (row[user_idx] if len(row) > user_idx else "").strip()
            s = (row[sid_idx]  if len(row) > sid_idx  else "").strip()
            if s and u and not u.startswith("#"):
                out.append(f"{u}|{s}")
            elif s and not s.startswith("#"):
                out.append(s)
        return out

    # C) single column fallback (first column)
    for row in rows:
        if not row:
            continue
        val = (row[0] or "").strip()
        if val and not val.startswith("#"):
            out.append(val)
    return out

def write_lines(path: str, lines: list[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for x in lines:
            f.write(x.strip() + "\n")

def main():
    # PROFILES
    if PROFILES_URL:
        try:
            profs = _parse_profiles(_fetch_csv_text(PROFILES_URL))
            write_lines(os.path.join(ROOT, "profiles.txt"), profs)
            print(f"Wrote profiles.txt with {len(profs)} line(s).")
        except Exception as e:
            print(f"⚠️ Profiles fetch failed: {e}")
    else:
        print("GSHEET_PROFILES_CSV not set; skipped profiles.txt.")

    # SESSIONS
    if SESSIONS_URL:
        try:
            sess = _parse_sessions(_fetch_csv_text(SESSIONS_URL))
            write_lines(os.path.join(ROOT, "sessions.txt"), sess)
            print(f"Wrote sessions.txt with {len(sess)} line(s).")
        except Exception as e:
            print(f"⚠️ Sessions fetch failed: {e}")
    else:
        print("GSHEET_SESSIONS_CSV not set; skipped sessions.txt.")

if __name__ == "__main__":
    main()
