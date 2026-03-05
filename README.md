# file_monitor

Rust service that watches configured directories, waits for file activity to go quiet, then optionally runs a script and/or sends an email. All scan decisions and action runs are logged to SQLite.

## How it works

For each `[[watch]]` record:

1. Recursively scans files under `path`.
2. Finds the latest file modification time (`mtime`).
3. Waits until that `mtime` has stopped changing for `quiet_minutes`.
4. Triggers once for that stable `mtime`.
5. Will not trigger again until a newer `mtime` appears.

If `script` is configured, it is executed with:

- `$1` = absolute path to the most recently updated file.

## Features

- SQLite-backed state and logs
- Polling scheduler (`poll_interval_seconds`)
- Quiet-window trigger detection
- Optional script execution
- Optional SMTP email notifications
- Script timeout and SMTP timeout controls
- Duplicate trigger protection via `(watch_id, trigger_mtime)`

## Requirements

- Rust toolchain (project currently tested with Rust `1.93.1`)
- SQLite (bundled via `sqlx`/`libsqlite3-sys` in build)
- Network access to SMTP server (if using email)

## Configuration

Use `monitor.toml` (or pass a custom path as first CLI arg).

```toml
[global]
poll_interval_seconds = 10
sqlite_path = "file_monitor.db"
smtp_timeout_seconds = 20
smtp_profile = "default"

[smtp.default]
host = "smtp.gmail.com"
port = 587
from = "you@gmail.com"
username_env = "FM_SMTP_USER"
password_env = "FM_SMTP_PASS"

[[watch]]
id = "downloads"
path = "/your/path"
quiet_minutes = 1
enabled = true

# Optional script
script = "/your/path/to/script.sh"
script_timeout_seconds = 600

# Optional email
email_to = ["you@gmail.com"]
email_subject = "Downloads settled"
```

### Optional behavior matrix

- script + email: both run
- script only: email skipped
- email only: script skipped
- neither: decision is logged only

## Run

```bash
cargo run -- monitor.toml
```

With verbose logging:

```bash
RUST_LOG=file_monitor=debug cargo run -- monitor.toml
```

With SMTP transport traces:

```bash
RUST_LOG=file_monitor=debug,lettre=trace cargo run -- monitor.toml
```

## Gmail notes

- SMTP host: `smtp.gmail.com`
- Port: `587` (STARTTLS) or `465` (TLS)
- Username: full Gmail address
- Password: App Password (when 2FA enabled)
- `from` should usually match the authenticated Gmail address

## Database tables

Created automatically at startup:

- `watches`: persisted state per watch (`last_seen_mtime`, `stable_since`, `last_triggered_mtime`)
- `scan_events`: every scan decision (`NoFiles`, `ActivityInProgress`, `QuietWindowMet`, `AlreadyProcessedMtime`)
- `action_runs`: script/email execution results per trigger

## Local dev notes

Ignored local files:

- `target/`
- `monitor.toml`
- `file_monitor.db`

Use `monitor.example.toml` as a template.
