# Pulling media from the Auto_Insta VPS

The Auto_Insta pipeline runs on your VPS and writes downloaded posts into the
shared downloads directory (typically `/srv/igdl/downloads`). Use the
`bin/pull_media_from_vps.py` helper on your laptop to synchronize those files to
local storage and maintain a timestamped log of every sync.

## Requirements

* Python 3.8 or newer
* `rsync` available on your `PATH`
* SSH access to the VPS (public key authentication is recommended)

## Quick start

1. **Clone or update this repository on your laptop.** Keep a copy of the
   latest code so the helper script matches whatever is running on the VPS.
2. **Verify prerequisites.** Confirm that `python3 --version` reports 3.8 or
   newer and that `rsync` is installed (`rsync --version`). Install them if
   needed.
3. **Test your SSH access.** Make sure you can reach the VPS with the account
   that has access to `/srv/igdl`. If you normally connect with
   `ssh root@vps.example.com`, the helper will be able to do the same.
4. **Decide on a local destination.** Pick or create the folder where the
   Instagram media should live (for example `~/AutoInstaDownloads`).
5. **Run a dry run.** Start with `--dry-run` so you can see what would sync
   without transferring files yet. Once the output looks correct, run again
   without `--dry-run` to download the media.
6. **Review the logs.** After each pull, inspect the timestamped log in
   `logs/pull_client/` for a summary of what changed.

## Basic usage

```bash
python3 bin/pull_media_from_vps.py <remote_host> <remote_path> <local_path>
```

Example:

```bash
python3 bin/pull_media_from_vps.py vps.example.com /srv/igdl/downloads ~/AutoInstaDownloads
```

This command creates the local folder if necessary, runs `rsync` in archive mode
to pull new and updated media, mirrors `/srv/igdl/media_log` alongside the
downloads (so `log_guard.py` sees the same CSVs), and writes a log file to
`logs/pull_client/` with a timestamped filename.

### Where to run the helper

* The script lives at `bin/pull_media_from_vps.py` inside this repository. Run it
  with `python3` and pass the path explicitly, e.g. `python3 /path/to/Auto_Insta/bin/pull_media_from_vps.py ...`.
* You can launch it from **any** working directory. The `local_path` argument
  controls where files are saved; relative paths are resolved against your
  current directory and the folder is created automatically if missing.
* If you prefer a shorter command, add the repository’s `bin/` folder to your
  `PATH` or create a small shell alias so you can simply call
  `pull_media_from_vps.py ...`.

## Options

* `--remote-user`: SSH user name (defaults to `root`).
* `--ssh-key`: Path to the private key used for SSH authentication.
* `--ssh-port`: Non-default SSH port.
* `--ssh-option`: Repeatable flag for passing extra parameters directly to the
  SSH command used by `rsync` (for example `--ssh-option -oStrictHostKeyChecking=no`).
* `--compress`: Enable `rsync` compression for slower connections.
* `--delete`: Mirror deletions from the VPS to the local folder.
* `--dry-run`: Show what would change without copying any files.
* `--log-file`: Write to a specific log file instead of the timestamped default.
* `--log-dir`: Custom directory for timestamped logs.
* `--rsync-arg`: Repeatable flag for appending additional arguments to the end
  of the `rsync` command.
* `--skip-media-log`: Skip mirroring `/srv/igdl/media_log` (mirroring is enabled by default).
* `--media-log-remote`: Override the remote media log directory when it does not
  live at `/srv/igdl/media_log`.
* `--media-log-local`: Customize where the media logs are stored locally. By
  default they are mirrored into a sibling directory named `media_log` so that
  `log_guard.py` can continue deduplicating based on the copied CSVs.

## Logging

Logs record the remote and local paths, the full `rsync` output, a summary of
how many files were downloaded or deleted, and whether the command completed
successfully.  Review `logs/pull_client/` after each run to see the transfer
history.

## Scheduling periodic pulls

On macOS or Linux you can combine the script with `cron`, `launchd`, or any task
scheduler of your choice. A simple cron entry that runs the sync every hour
looks like this:

```cron
0 * * * * /usr/bin/python3 /path/to/repo/bin/pull_media_from_vps.py \
    vps.example.com /srv/igdl/downloads /Users/you/AutoInstaDownloads >> \
    /path/to/repo/logs/pull_client/cron.log 2>&1
```

Adjust the cadence and paths as needed for your environment.
