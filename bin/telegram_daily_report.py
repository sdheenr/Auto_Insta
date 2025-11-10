#!/usr/bin/env python3
import os, sys, time, requests, pathlib

BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
CHAT_ID   = os.getenv("TG_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")

SERVER_SUMMARY = "/srv/igdl/server_last_run.log"
LAPTOP_LOG     = "/srv/igdl/laptop_push_pull_log.txt"
LAPTOP_FRESH_WINDOW_HOURS = 24
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_text(text, parse_mode=None):
    data = {"chat_id": CHAT_ID, "text": text}
    if parse_mode:
        data["parse_mode"] = parse_mode
        data["disable_web_page_preview"] = True
    r = requests.post(f"{API_BASE}/sendMessage", data=data, timeout=30)
    r.raise_for_status()

def send_document(path, caption=None):
    with open(path, "rb") as f:
        files = {"document": (os.path.basename(path), f)}
        data = {"chat_id": CHAT_ID}
        if caption:
            data["caption"] = caption
        r = requests.post(f"{API_BASE}/sendDocument", data=data, files=files, timeout=60)
        r.raise_for_status()

def read_text(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read().strip()

def is_fresh(path, hours):
    try:
        mtime = pathlib.Path(path).stat().st_mtime
    except FileNotFoundError:
        return False
    return (time.time() - mtime) <= hours * 3600

def main():
    if "PUT_YOUR_BOT_TOKEN_HERE" in BOT_TOKEN or "PUT_YOUR_CHAT_ID_HERE" in CHAT_ID:
        print("Set TG_BOT_TOKEN and TG_CHAT_ID (env) or edit the script.", file=sys.stderr)
        sys.exit(2)

    # Server summary first
    if os.path.exists(SERVER_SUMMARY):
        txt = read_text(SERVER_SUMMARY)
        msg = f"📣 IGDL Daily Report\n\n🖥️ *Server summary*\n```\n{txt}\n```"
        if len(msg) <= 3800:
            send_text(msg, parse_mode="Markdown")
        else:
            send_text("📣 IGDL Daily Report\n🖥️ Server summary is long — attaching as file.")
            send_document(SERVER_SUMMARY, caption="server_last_run.log")
    else:
        send_text("📣 IGDL Daily Report\n🖥️ No server_last_run.log found.")

    # Laptop log if fresh within 24h
    if is_fresh(LAPTOP_LOG, LAPTOP_FRESH_WINDOW_HOURS):
        try:
            tail = "\n".join(read_text(LAPTOP_LOG).splitlines()[-15:])
            teaser = f"💻 *Laptop log* (last 24h)\n```\n{tail}\n```"
            if len(teaser) <= 3800:
                send_text(teaser, parse_mode="Markdown")
            else:
                send_text("💻 Laptop log is large — attaching full file.")
            send_document(LAPTOP_LOG, caption="laptop_push_pull_log.txt (fresh)")
        except Exception as e:
            send_text(f"⚠️ Could not attach laptop log: {e}")
    else:
        send_text("ℹ️ No fresh laptop log in the last 24 hours.")

if __name__ == "__main__":
    main()
