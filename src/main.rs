use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use lettre::message::{Mailbox, MultiPart, SinglePart, header::ContentType};
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
    transport::smtp::authentication::Credentials,
};
use serde::Deserialize;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio::process::Command;
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default)]
    global: GlobalConfig,
    #[serde(default)]
    smtp: HashMap<String, SmtpProfile>,
    #[serde(default)]
    watch: Vec<WatchConfig>,
}

#[derive(Debug, Deserialize)]
struct GlobalConfig {
    #[serde(default = "default_poll_interval_seconds")]
    poll_interval_seconds: u64,
    #[serde(default = "default_sqlite_path")]
    sqlite_path: String,
    #[serde(default = "default_smtp_timeout_seconds")]
    smtp_timeout_seconds: u64,
    smtp_profile: Option<String>,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: default_poll_interval_seconds(),
            sqlite_path: default_sqlite_path(),
            smtp_timeout_seconds: default_smtp_timeout_seconds(),
            smtp_profile: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct SmtpProfile {
    host: String,
    #[serde(default = "default_smtp_port")]
    port: u16,
    from: String,
    username_env: Option<String>,
    password_env: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct WatchConfig {
    id: String,
    path: String,
    quiet_minutes: i64,
    #[serde(default = "default_enabled")]
    enabled: bool,
    script: Option<String>,
    #[serde(default = "default_script_timeout_seconds")]
    script_timeout_seconds: u64,
    #[serde(default)]
    email_to: Vec<String>,
    email_subject: Option<String>,
    smtp_profile: Option<String>,
}

#[derive(Debug)]
struct WatchState {
    last_seen_mtime: Option<DateTime<Utc>>,
    stable_since: Option<DateTime<Utc>>,
    last_triggered_mtime: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct ScanSnapshot {
    latest_mtime: Option<DateTime<Utc>>,
    latest_path: Option<String>,
    file_count: u64,
}

#[derive(Debug)]
struct ScriptResult {
    executed: bool,
    ok: bool,
    status_code: Option<i32>,
    timed_out: bool,
    stdout_excerpt: Option<String>,
    stderr_excerpt: Option<String>,
    error: Option<String>,
}

#[derive(Debug)]
struct EmailResult {
    sent: bool,
    ok: bool,
    error: Option<String>,
}

fn default_poll_interval_seconds() -> u64 {
    30
}

fn default_sqlite_path() -> String {
    "file_monitor.db".to_string()
}

fn default_smtp_port() -> u16 {
    587
}

fn default_smtp_timeout_seconds() -> u64 {
    20
}

fn default_enabled() -> bool {
    true
}

fn default_script_timeout_seconds() -> u64 {
    600
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,file_monitor=info".to_string()),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "monitor.toml".to_string());

    let config = read_config(&config_path)?;
    validate_config(&config)?;

    let pool = open_db(&config.global.sqlite_path).await?;
    init_db(&pool).await?;
    sync_watch_rows(&pool, &config.watch).await?;

    info!(
        config_path = config_path,
        poll_interval_seconds = config.global.poll_interval_seconds,
        smtp_timeout_seconds = config.global.smtp_timeout_seconds,
        watch_count = config.watch.len(),
        "file monitor started"
    );
    for watch in &config.watch {
        info!(
            watch_id = watch.id,
            path = watch.path,
            quiet_minutes = watch.quiet_minutes,
            enabled = watch.enabled,
            script_enabled = watch.script.is_some(),
            email_enabled = !watch.email_to.is_empty(),
            "watch config loaded"
        );
    }

    let mut ticker = interval(Duration::from_secs(config.global.poll_interval_seconds));

    loop {
        ticker.tick().await;
        info!("scheduler tick");

        for watch in &config.watch {
            if !watch.enabled {
                debug!(watch_id = watch.id, "watch disabled; skipping");
                continue;
            }

            if let Err(err) = process_watch(&pool, &config, watch).await {
                error!(watch_id = watch.id, error = %err, "watch processing failed");
            }
        }
    }
}

fn read_config(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file at {}", path))?;
    toml::from_str(&content).with_context(|| format!("failed to parse TOML in {}", path))
}

fn validate_config(config: &Config) -> Result<()> {
    if config.watch.is_empty() {
        return Err(anyhow!("config has no [[watch]] entries"));
    }

    let mut seen = std::collections::HashSet::new();
    for watch in &config.watch {
        if watch.id.trim().is_empty() {
            return Err(anyhow!("watch id cannot be empty"));
        }
        if !seen.insert(watch.id.clone()) {
            return Err(anyhow!("duplicate watch id: {}", watch.id));
        }
        if watch.quiet_minutes <= 0 {
            return Err(anyhow!(
                "watch {} has invalid quiet_minutes {}",
                watch.id,
                watch.quiet_minutes
            ));
        }

        if !watch.email_to.is_empty() {
            let profile_name = selected_smtp_profile(config, watch)?;
            if !config.smtp.contains_key(&profile_name) {
                return Err(anyhow!(
                    "watch {} references SMTP profile '{}' but it is not configured",
                    watch.id,
                    profile_name
                ));
            }
        }
    }

    Ok(())
}

async fn open_db(sqlite_path: &str) -> Result<SqlitePool> {
    let options = if sqlite_path.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(sqlite_path)
            .with_context(|| format!("invalid sqlite connection string '{}'", sqlite_path))?
    } else {
        SqliteConnectOptions::new().filename(sqlite_path)
    }
    .create_if_missing(true)
    .foreign_keys(true);

    SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .with_context(|| format!("failed to connect sqlite at {}", sqlite_path))
}

async fn init_db(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS watches (
            watch_id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            quiet_minutes INTEGER NOT NULL,
            last_seen_mtime TEXT NULL,
            stable_since TEXT NULL,
            last_triggered_mtime TEXT NULL,
            updated_at TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS scan_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            watch_id TEXT NOT NULL,
            scanned_at TEXT NOT NULL,
            latest_mtime TEXT NULL,
            latest_path TEXT NULL,
            file_count INTEGER NOT NULL,
            decision TEXT NOT NULL,
            note TEXT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS action_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            watch_id TEXT NOT NULL,
            trigger_mtime TEXT NOT NULL,
            latest_file_path TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NULL,
            script_configured INTEGER NOT NULL,
            script_executed INTEGER NOT NULL,
            script_ok INTEGER NULL,
            script_status_code INTEGER NULL,
            script_timed_out INTEGER NOT NULL,
            script_stdout_excerpt TEXT NULL,
            script_stderr_excerpt TEXT NULL,
            email_configured INTEGER NOT NULL,
            email_sent INTEGER NOT NULL,
            email_ok INTEGER NULL,
            error_summary TEXT NULL,
            UNIQUE(watch_id, trigger_mtime)
        )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn sync_watch_rows(pool: &SqlitePool, watches: &[WatchConfig]) -> Result<()> {
    for watch in watches {
        sqlx::query(
            r#"
            INSERT INTO watches (watch_id, path, quiet_minutes, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(watch_id) DO UPDATE SET
                path = excluded.path,
                quiet_minutes = excluded.quiet_minutes,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&watch.id)
        .bind(&watch.path)
        .bind(watch.quiet_minutes)
        .bind(Utc::now().to_rfc3339())
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn process_watch(pool: &SqlitePool, config: &Config, watch: &WatchConfig) -> Result<()> {
    let now = Utc::now();
    let scan = scan_directory(&watch.path)?;
    let state = load_watch_state(pool, &watch.id).await?;
    debug!(
        watch_id = watch.id,
        file_count = scan.file_count,
        latest_mtime = ?scan.latest_mtime,
        latest_path = ?scan.latest_path,
        prev_last_seen = ?state.last_seen_mtime,
        prev_stable_since = ?state.stable_since,
        prev_last_triggered = ?state.last_triggered_mtime,
        "scan completed; evaluating state"
    );

    let (decision, note, should_trigger) = match scan.latest_mtime {
        None => {
            reset_watch_candidate_state(pool, &watch.id, now).await?;
            ("NoFiles", None, false)
        }
        Some(latest_mtime) => {
            if state.last_seen_mtime != Some(latest_mtime) {
                set_watch_candidate(pool, &watch.id, latest_mtime, now).await?;
                (
                    "ActivityInProgress",
                    Some("latest mtime changed; waiting for quiet window".to_string()),
                    false,
                )
            } else if state.last_triggered_mtime == Some(latest_mtime) {
                ("AlreadyProcessedMtime", None, false)
            } else if let Some(stable_since) = state.stable_since {
                let elapsed_seconds = now.signed_duration_since(stable_since).num_seconds();
                if elapsed_seconds >= watch.quiet_minutes * 60 {
                    ("QuietWindowMet", None, true)
                } else {
                    (
                        "ActivityInProgress",
                        Some(format!(
                            "quiet window in progress: {} / {} seconds",
                            elapsed_seconds,
                            watch.quiet_minutes * 60
                        )),
                        false,
                    )
                }
            } else {
                set_stable_since(pool, &watch.id, now).await?;
                (
                    "ActivityInProgress",
                    Some("missing stable_since recovered".to_string()),
                    false,
                )
            }
        }
    };

    log_scan_event(pool, &watch.id, &scan, decision, note.as_deref()).await?;
    info!(
        watch_id = watch.id,
        decision = decision,
        should_trigger = should_trigger,
        latest_mtime = ?scan.latest_mtime,
        latest_path = ?scan.latest_path,
        note = ?note,
        "watch decision"
    );

    if should_trigger {
        let trigger_mtime = scan
            .latest_mtime
            .ok_or_else(|| anyhow!("internal error: expected latest_mtime for trigger"))?;
        let latest_file = scan
            .latest_path
            .clone()
            .ok_or_else(|| anyhow!("internal error: expected latest_path for trigger"))?;

        run_actions(pool, config, watch, trigger_mtime, &latest_file).await?;
        set_last_triggered(pool, &watch.id, trigger_mtime).await?;

        info!(
            watch_id = watch.id,
            trigger_mtime = %trigger_mtime,
            latest_file = latest_file,
            "watch triggered"
        );
    }

    Ok(())
}

fn scan_directory(path: &str) -> Result<ScanSnapshot> {
    let started = Instant::now();
    let mut file_count = 0_u64;
    let mut latest: Option<(DateTime<Utc>, String)> = None;

    for entry in WalkDir::new(path).into_iter() {
        match entry {
            Ok(entry) => {
                if !entry.file_type().is_file() {
                    continue;
                }

                file_count += 1;
                let entry_path = entry.path();
                let metadata = match entry.metadata() {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(path = %entry_path.display(), error = %err, "failed reading metadata");
                        continue;
                    }
                };

                let modified = match metadata.modified() {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(path = %entry_path.display(), error = %err, "failed reading modified time");
                        continue;
                    }
                };

                let modified_utc: DateTime<Utc> = system_time_to_utc(modified);
                let entry_path_str = entry_path.to_string_lossy().to_string();

                match &latest {
                    None => latest = Some((modified_utc, entry_path_str)),
                    Some((current_mtime, current_path)) => {
                        if modified_utc > *current_mtime
                            || (modified_utc == *current_mtime && entry_path_str < *current_path)
                        {
                            latest = Some((modified_utc, entry_path_str));
                        }
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "directory walk entry failed");
            }
        }
    }

    let (latest_mtime, latest_path) = match latest {
        Some((mtime, path_str)) => (Some(mtime), Some(path_str)),
        None => (None, None),
    };
    debug!(
        path = path,
        file_count = file_count,
        latest_mtime = ?latest_mtime,
        latest_path = ?latest_path,
        elapsed_ms = started.elapsed().as_millis(),
        "directory scan stats"
    );

    Ok(ScanSnapshot {
        latest_mtime,
        latest_path,
        file_count,
    })
}

async fn load_watch_state(pool: &SqlitePool, watch_id: &str) -> Result<WatchState> {
    let row = sqlx::query(
        "SELECT last_seen_mtime, stable_since, last_triggered_mtime FROM watches WHERE watch_id = ?",
    )
    .bind(watch_id)
    .fetch_optional(pool)
    .await?;

    let row = row.ok_or_else(|| anyhow!("watch state missing for id {}", watch_id))?;

    Ok(WatchState {
        last_seen_mtime: parse_optional_rfc3339(row.get::<Option<String>, _>("last_seen_mtime")),
        stable_since: parse_optional_rfc3339(row.get::<Option<String>, _>("stable_since")),
        last_triggered_mtime: parse_optional_rfc3339(
            row.get::<Option<String>, _>("last_triggered_mtime"),
        ),
    })
}

fn parse_optional_rfc3339(value: Option<String>) -> Option<DateTime<Utc>> {
    value.and_then(|v| {
        DateTime::parse_from_rfc3339(&v)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    })
}

async fn set_watch_candidate(
    pool: &SqlitePool,
    watch_id: &str,
    latest_mtime: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE watches
        SET last_seen_mtime = ?, stable_since = ?, updated_at = ?
        WHERE watch_id = ?
        "#,
    )
    .bind(latest_mtime.to_rfc3339())
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .bind(watch_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_stable_since(pool: &SqlitePool, watch_id: &str, now: DateTime<Utc>) -> Result<()> {
    sqlx::query("UPDATE watches SET stable_since = ?, updated_at = ? WHERE watch_id = ?")
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(watch_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn reset_watch_candidate_state(
    pool: &SqlitePool,
    watch_id: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        "UPDATE watches SET last_seen_mtime = NULL, stable_since = NULL, updated_at = ? WHERE watch_id = ?",
    )
    .bind(now.to_rfc3339())
    .bind(watch_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_last_triggered(
    pool: &SqlitePool,
    watch_id: &str,
    trigger_mtime: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        "UPDATE watches SET last_triggered_mtime = ?, updated_at = ? WHERE watch_id = ?",
    )
    .bind(trigger_mtime.to_rfc3339())
    .bind(Utc::now().to_rfc3339())
    .bind(watch_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn log_scan_event(
    pool: &SqlitePool,
    watch_id: &str,
    scan: &ScanSnapshot,
    decision: &str,
    note: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO scan_events (watch_id, scanned_at, latest_mtime, latest_path, file_count, decision, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(watch_id)
    .bind(Utc::now().to_rfc3339())
    .bind(scan.latest_mtime.map(|v| v.to_rfc3339()))
    .bind(scan.latest_path.as_deref())
    .bind(scan.file_count as i64)
    .bind(decision)
    .bind(note)
    .execute(pool)
    .await?;
    Ok(())
}

async fn run_actions(
    pool: &SqlitePool,
    config: &Config,
    watch: &WatchConfig,
    trigger_mtime: DateTime<Utc>,
    latest_file: &str,
) -> Result<()> {
    let script_configured = watch.script.is_some();
    let email_configured = !watch.email_to.is_empty();
    info!(
        watch_id = watch.id,
        trigger_mtime = %trigger_mtime,
        latest_file = latest_file,
        script_configured = script_configured,
        email_configured = email_configured,
        "starting action run"
    );

    let insert = sqlx::query(
        r#"
        INSERT INTO action_runs (
            watch_id, trigger_mtime, latest_file_path, started_at,
            script_configured, script_executed, script_timed_out,
            email_configured, email_sent
        )
        VALUES (?, ?, ?, ?, ?, 0, 0, ?, 0)
        ON CONFLICT(watch_id, trigger_mtime) DO NOTHING
        "#,
    )
    .bind(&watch.id)
    .bind(trigger_mtime.to_rfc3339())
    .bind(latest_file)
    .bind(Utc::now().to_rfc3339())
    .bind(bool_to_i64(script_configured))
    .bind(bool_to_i64(email_configured))
    .execute(pool)
    .await?;

    if insert.rows_affected() == 0 {
        info!(watch_id = watch.id, trigger_mtime = %trigger_mtime, "action already recorded");
        return Ok(());
    }

    let mut script_result: Option<ScriptResult> = None;
    if script_configured {
        let script = watch.script.as_deref().unwrap_or_default();
        info!(
            watch_id = watch.id,
            script = script,
            arg1 = latest_file,
            timeout_seconds = watch.script_timeout_seconds,
            "executing script"
        );
        script_result = Some(run_script(script, latest_file, watch.script_timeout_seconds).await);
    }

    let mut email_result: Option<EmailResult> = None;
    if email_configured {
        info!(
            watch_id = watch.id,
            recipient_count = watch.email_to.len(),
            "sending email"
        );
        email_result = Some(send_email(config, watch, trigger_mtime, latest_file, script_result.as_ref()).await);
    }

    let script_executed = script_result.as_ref().map(|r| r.executed).unwrap_or(false);
    let script_ok = script_result.as_ref().map(|r| r.ok);
    let script_status_code = script_result.as_ref().and_then(|r| r.status_code);
    let script_timed_out = script_result.as_ref().map(|r| r.timed_out).unwrap_or(false);
    let script_stdout_excerpt = script_result.as_ref().and_then(|r| r.stdout_excerpt.as_deref());
    let script_stderr_excerpt = script_result.as_ref().and_then(|r| r.stderr_excerpt.as_deref());

    let email_sent = email_result.as_ref().map(|r| r.sent).unwrap_or(false);
    let email_ok = email_result.as_ref().map(|r| r.ok);

    let mut errors = Vec::new();
    if let Some(script_err) = script_result.as_ref().and_then(|r| r.error.as_ref()) {
        errors.push(format!("script: {}", script_err));
    }
    if let Some(email_err) = email_result.as_ref().and_then(|r| r.error.as_ref()) {
        errors.push(format!("email: {}", email_err));
    }
    let error_summary = if errors.is_empty() {
        None
    } else {
        Some(errors.join(" | "))
    };

    sqlx::query(
        r#"
        UPDATE action_runs
        SET finished_at = ?,
            script_executed = ?,
            script_ok = ?,
            script_status_code = ?,
            script_timed_out = ?,
            script_stdout_excerpt = ?,
            script_stderr_excerpt = ?,
            email_sent = ?,
            email_ok = ?,
            error_summary = ?
        WHERE watch_id = ? AND trigger_mtime = ?
        "#,
    )
    .bind(Utc::now().to_rfc3339())
    .bind(bool_to_i64(script_executed))
    .bind(script_ok.map(bool_to_i64))
    .bind(script_status_code)
    .bind(bool_to_i64(script_timed_out))
    .bind(script_stdout_excerpt)
    .bind(script_stderr_excerpt)
    .bind(bool_to_i64(email_sent))
    .bind(email_ok.map(bool_to_i64))
    .bind(error_summary.as_deref())
    .bind(&watch.id)
    .bind(trigger_mtime.to_rfc3339())
    .execute(pool)
    .await?;
    info!(
        watch_id = watch.id,
        trigger_mtime = %trigger_mtime,
        script_ok = ?script_ok,
        email_ok = ?email_ok,
        error_summary = ?error_summary,
        "action run completed"
    );

    Ok(())
}

async fn run_script(script: &str, latest_file: &str, timeout_seconds: u64) -> ScriptResult {
    let started = Instant::now();
    let mut cmd = Command::new(script);
    cmd.arg(latest_file);
    cmd.kill_on_drop(true);

    let output_result = timeout(Duration::from_secs(timeout_seconds), cmd.output()).await;

    let result = match output_result {
        Ok(Ok(output)) => {
            let status_code = output.status.code();
            let ok = output.status.success();
            ScriptResult {
                executed: true,
                ok,
                status_code,
                timed_out: false,
                stdout_excerpt: Some(truncate_lossy_utf8(&output.stdout, 2000)),
                stderr_excerpt: Some(truncate_lossy_utf8(&output.stderr, 2000)),
                error: if ok {
                    None
                } else {
                    Some(format!(
                        "script exited unsuccessfully with code {:?}",
                        status_code
                    ))
                },
            }
        }
        Ok(Err(err)) => ScriptResult {
            executed: true,
            ok: false,
            status_code: None,
            timed_out: false,
            stdout_excerpt: None,
            stderr_excerpt: None,
            error: Some(format!("failed running script: {}", err)),
        },
        Err(_) => ScriptResult {
            executed: true,
            ok: false,
            status_code: None,
            timed_out: true,
            stdout_excerpt: None,
            stderr_excerpt: None,
            error: Some(format!(
                "script timed out after {} seconds",
                timeout_seconds
            )),
        },
    };

    info!(
        script = script,
        arg1 = latest_file,
        ok = result.ok,
        timed_out = result.timed_out,
        status_code = ?result.status_code,
        elapsed_ms = started.elapsed().as_millis(),
        error = ?result.error,
        "script finished"
    );

    result
}

async fn send_email(
    config: &Config,
    watch: &WatchConfig,
    trigger_mtime: DateTime<Utc>,
    latest_file: &str,
    script_result: Option<&ScriptResult>,
) -> EmailResult {
    match send_email_inner(config, watch, trigger_mtime, latest_file, script_result).await {
        Ok(()) => EmailResult {
            sent: true,
            ok: true,
            error: None,
        },
        Err(err) => EmailResult {
            sent: true,
            ok: false,
            error: Some(err.to_string()),
        },
    }
}

async fn send_email_inner(
    config: &Config,
    watch: &WatchConfig,
    trigger_mtime: DateTime<Utc>,
    latest_file: &str,
    script_result: Option<&ScriptResult>,
) -> Result<()> {
    let profile_name = selected_smtp_profile(config, watch)?;
    let profile = config
        .smtp
        .get(&profile_name)
        .ok_or_else(|| anyhow!("smtp profile '{}' not found", profile_name))?;
    debug!(
        watch_id = watch.id,
        smtp_profile = profile_name,
        smtp_host = profile.host,
        smtp_port = profile.port,
        recipient_count = watch.email_to.len(),
        "resolved smtp profile"
    );

    let from: Mailbox = profile
        .from
        .parse()
        .with_context(|| format!("invalid SMTP from address '{}" , profile.from))?;

    let mut message_builder = Message::builder().from(from);
    for email in &watch.email_to {
        let mailbox: Mailbox = email
            .parse()
            .with_context(|| format!("invalid email recipient '{}'", email))?;
        message_builder = message_builder.to(mailbox);
    }

    let subject = watch
        .email_subject
        .clone()
        .unwrap_or_else(|| format!("file_monitor: {} quiet window met", watch.id));

    let body = render_email_body(watch, trigger_mtime, latest_file, script_result);

    let message = message_builder
        .subject(subject)
        .multipart(
            MultiPart::alternative().singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(body),
            ),
        )?;

    let mut transport_builder = if profile.port == 587 {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&profile.host)
            .with_context(|| format!("invalid SMTP STARTTLS relay host '{}'", profile.host))?
            .port(profile.port)
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::relay(&profile.host)
            .with_context(|| format!("invalid SMTP TLS relay host '{}'", profile.host))?
            .port(profile.port)
    };
    debug!(
        watch_id = watch.id,
        smtp_host = profile.host,
        smtp_port = profile.port,
        smtp_mode = if profile.port == 587 {
            "starttls"
        } else {
            "tls"
        },
        "initialized SMTP transport"
    );

    if let (Some(username_env), Some(password_env)) = (&profile.username_env, &profile.password_env) {
        let username = std::env::var(username_env)
            .with_context(|| format!("missing SMTP username env {}", username_env))?;
        let password = std::env::var(password_env)
            .with_context(|| format!("missing SMTP password env {}", password_env))?;
        debug!(
            watch_id = watch.id,
            username_env = username_env,
            password_env = password_env,
            username_len = username.len(),
            "SMTP credentials resolved from env"
        );
        transport_builder = transport_builder.credentials(Credentials::new(username, password));
    } else {
        warn!(
            watch_id = watch.id,
            "SMTP credentials env vars are not fully configured; attempting unauthenticated send"
        );
    }

    let mailer = transport_builder.build();
    let smtp_timeout = Duration::from_secs(config.global.smtp_timeout_seconds);
    info!(
        watch_id = watch.id,
        timeout_seconds = config.global.smtp_timeout_seconds,
        "sending SMTP message"
    );
    let send_result = timeout(smtp_timeout, mailer.send(message)).await;
    match send_result {
        Ok(Ok(_response)) => {
            info!(watch_id = watch.id, "SMTP send completed");
        }
        Ok(Err(err)) => {
            return Err(anyhow!("SMTP send failed: {}", err));
        }
        Err(_) => {
            return Err(anyhow!(
                "SMTP send timed out after {} seconds",
                config.global.smtp_timeout_seconds
            ));
        }
    }

    Ok(())
}

fn render_email_body(
    watch: &WatchConfig,
    trigger_mtime: DateTime<Utc>,
    latest_file: &str,
    script_result: Option<&ScriptResult>,
) -> String {
    let mut lines = vec![
        format!("watch_id: {}", watch.id),
        format!("path: {}", watch.path),
        format!("trigger_mtime_utc: {}", trigger_mtime.to_rfc3339()),
        format!("latest_file: {}", latest_file),
    ];

    if let Some(script) = script_result {
        lines.push("".to_string());
        lines.push("script_result:".to_string());
        lines.push(format!("  ok: {}", script.ok));
        lines.push(format!("  timed_out: {}", script.timed_out));
        lines.push(format!("  status_code: {:?}", script.status_code));
        if let Some(err) = &script.error {
            lines.push(format!("  error: {}", err));
        }
        if let Some(stderr) = &script.stderr_excerpt {
            if !stderr.is_empty() {
                lines.push(format!("  stderr_excerpt: {}", stderr));
            }
        }
    }

    lines.join("\n")
}

fn selected_smtp_profile(config: &Config, watch: &WatchConfig) -> Result<String> {
    if let Some(profile) = &watch.smtp_profile {
        return Ok(profile.clone());
    }
    if let Some(profile) = &config.global.smtp_profile {
        return Ok(profile.clone());
    }
    Ok("default".to_string())
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn truncate_lossy_utf8(bytes: &[u8], max_chars: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.chars().count() <= max_chars {
        text.into_owned()
    } else {
        text.chars().take(max_chars).collect()
    }
}

fn system_time_to_utc(time: SystemTime) -> DateTime<Utc> {
    DateTime::<Utc>::from(time)
}

#[allow(dead_code)]
fn canonicalize_or_original(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}
