#!/usr/bin/env python3
"""Synchronize downloaded media from a remote Auto_Insta VPS to a local machine.

This helper script is intended to run on a laptop or workstation.  It wraps
``rsync`` to copy the shared downloads directory (and optionally log files)
from the VPS to the local filesystem while keeping a timestamped log of every
transfer.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Tuple

_LOGGER_NAME = "pull_media_from_vps"


def _configure_logging(log_file: Path) -> logging.Logger:
    """Configure a logger that writes both to stdout and ``log_file``."""

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Always attach a file handler so every run is persisted.
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Mirror messages to stdout for interactive runs.
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def _rsync_available() -> bool:
    """Return ``True`` when ``rsync`` is available on ``PATH``."""

    return shutil.which("rsync") is not None


def _default_log_file(log_dir: Path) -> Path:
    timestamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return log_dir / f"pull_{timestamp}.log"


def _build_ssh_command(args: argparse.Namespace) -> str:
    parts = ["ssh"]
    if args.ssh_port:
        parts.extend(["-p", str(args.ssh_port)])
    if args.ssh_key:
        parts.extend(["-i", str(args.ssh_key)])
    if args.ssh_options:
        parts.extend(args.ssh_options)
    return " ".join(parts)


def _default_media_log_dir(local_download_path: Path) -> Path:
    """Choose a sensible default for the local media-log mirror."""

    # Keep the layout similar to the VPS: downloads in one directory, media_log
    # as a sibling directory.  If ``local_download_path`` has no sensible
    # parent (e.g. it is ``/``), fall back to placing ``media_log`` inside the
    # downloads directory to avoid requiring root permissions.
    parent = local_download_path.parent
    if parent == local_download_path:
        return local_download_path / "media_log"
    return parent / "media_log"


def _build_rsync_command(
    args: argparse.Namespace,
    ssh_command: str,
    remote_path: str,
    local_path: Path,
) -> Tuple[str, ...]:
    remote = f"{args.remote_user}@{args.remote_host}:{remote_path}"
    command = [
        "rsync",
        "-avh",  # verbose, archive mode, human readable sizes
        "-i",  # itemized changes to make parsing easier
        "--partial",  # resume partial transfers if interrupted
        "--prune-empty-dirs",
    ]
    if args.compress:
        command.append("--compress")
    if args.delete:
        command.append("--delete")
    if args.dry_run:
        command.append("--dry-run")
    if ssh_command:
        command.extend(["-e", ssh_command])
    if args.extra_rsync_args:
        command.extend(args.extra_rsync_args)
    command.extend([remote, str(local_path)])
    return tuple(command)


def _summarize_changes(lines: Iterable[str]) -> Tuple[int, int]:
    """Count downloaded and deleted files from rsync's ``-i`` output."""

    downloaded = 0
    deleted = 0
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith(("sending ", "receiving ", "sent ", "total size")):
            continue
        if line.startswith("*deleting"):
            deleted += 1
        elif line[0] in {">", "c"} and line[1:2] == "f":
            # Files flagged with >f (transferred) or cf (created) count as downloads
            downloaded += 1
    return downloaded, deleted


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pull Instagram media downloaded on the Auto_Insta VPS to the local "
            "machine using rsync."
        )
    )
    parser.add_argument("remote_host", help="Hostname or IP address of the VPS")
    parser.add_argument(
        "remote_path",
        help=(
            "Remote path that contains the shared downloads directory. For "
            "example: /srv/igdl/downloads"
        ),
    )
    parser.add_argument(
        "local_path",
        type=Path,
        help="Local directory where the media should be stored.",
    )
    parser.add_argument(
        "--remote-user",
        default="root",
        help="SSH username to authenticate as (default: %(default)s)",
    )
    parser.add_argument(
        "--ssh-key",
        type=Path,
        help="Path to the SSH private key to use for authentication.",
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        help="Non-default SSH port if the VPS does not use 22.",
    )
    parser.add_argument(
        "--ssh-option",
        dest="ssh_options",
        action="append",
        help=(
            "Repeatable. Additional options passed verbatim to the ssh command "
            "used by rsync."
        ),
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Enable rsync compression (useful on slower links).",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Mirror deletions from the VPS to the local directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a trial run with no changes to verify what would sync.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Write logs to this path instead of auto-generating a timestamped name.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("logs") / "pull_client",
        help=(
            "Directory where timestamped logs should be stored when --log-file "
            "is not provided (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--rsync-arg",
        dest="extra_rsync_args",
        action="append",
        help="Repeatable. Additional arguments appended to the rsync command.",
    )
    parser.add_argument(
        "--skip-media-log",
        action="store_true",
        help="Skip mirroring the VPS media_log directory.",
    )
    parser.add_argument(
        "--media-log-remote",
        default="/srv/igdl/media_log",
        help="Remote directory that stores media log CSVs (default: %(default)s).",
    )
    parser.add_argument(
        "--media-log-local",
        type=Path,
        help=(
            "Local directory where media log CSVs should be stored. Defaults to a "
            "sibling of the download directory named 'media_log'."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    if not _rsync_available():
        print("Error: rsync is required but not found on PATH.", file=sys.stderr)
        return 2

    log_file = args.log_file
    if log_file is None:
        log_dir = args.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = _default_log_file(log_dir)
    else:
        log_file = log_file.expanduser()
        log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = _configure_logging(log_file)

    local_path = args.local_path.expanduser().resolve()
    local_path.mkdir(parents=True, exist_ok=True)

    media_log_local: Path | None
    sync_media_log = not args.skip_media_log and bool(args.media_log_remote)
    if sync_media_log:
        if args.media_log_local is not None:
            media_log_local = args.media_log_local.expanduser().resolve()
        else:
            media_log_local = _default_media_log_dir(local_path)
        media_log_local.mkdir(parents=True, exist_ok=True)
    else:
        media_log_local = None

    if args.ssh_key:
        args.ssh_key = args.ssh_key.expanduser()

    ssh_command = _build_ssh_command(args)
    rsync_command = _build_rsync_command(args, ssh_command, args.remote_path, local_path)

    logger.info("Starting synchronization.")
    logger.info("Remote: %s@%s:%s", args.remote_user, args.remote_host, args.remote_path)
    logger.info("Local: %s", local_path)
    if args.dry_run:
        logger.info("Dry-run mode enabled. No files will be modified.")

    logger.debug("Executing command: %s", " ".join(rsync_command))

    try:
        completed = subprocess.run(
            rsync_command,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        logger.error("Failed to launch rsync: %s", exc)
        return 1

    stdout_lines = completed.stdout.splitlines() if completed.stdout else []
    stderr_lines = completed.stderr.splitlines() if completed.stderr else []

    for line in stdout_lines:
        logger.info("[rsync] %s", line)
    for line in stderr_lines:
        logger.warning("[rsync] %s", line)

    downloaded, deleted = _summarize_changes(stdout_lines)
    if downloaded or deleted:
        logger.info("Summary: %d file(s) downloaded, %d file(s) deleted.", downloaded, deleted)

    if completed.returncode != 0:
        logger.error("rsync exited with status %s", completed.returncode)
        return completed.returncode

    if sync_media_log and media_log_local is not None:
        logger.info(
            "Synchronizing media log CSVs to %s (remote: %s)",
            media_log_local,
            args.media_log_remote,
        )
        log_command = _build_rsync_command(
            args,
            ssh_command,
            args.media_log_remote,
            media_log_local,
        )
        logger.debug("Executing command: %s", " ".join(log_command))
        try:
            log_completed = subprocess.run(
                log_command,
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as exc:
            logger.warning("Failed to synchronize media logs: %s", exc)
        else:
            log_stdout = log_completed.stdout.splitlines() if log_completed.stdout else []
            log_stderr = log_completed.stderr.splitlines() if log_completed.stderr else []
            for line in log_stdout:
                logger.info("[rsync-media-log] %s", line)
            for line in log_stderr:
                logger.warning("[rsync-media-log] %s", line)

            log_downloaded, log_deleted = _summarize_changes(log_stdout)
            if log_downloaded or log_deleted:
                logger.info(
                    "Media log summary: %d file(s) downloaded, %d file(s) deleted.",
                    log_downloaded,
                    log_deleted,
                )

            if log_completed.returncode != 0:
                logger.warning(
                    "Media log synchronization exited with status %s", log_completed.returncode
                )

    else:
        logger.info("Media log synchronization skipped.")

    logger.info("Synchronization finished successfully. Log saved to %s", log_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
