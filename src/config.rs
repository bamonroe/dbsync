//! Loading and validating `config.toml`.
//!
//! The example that ships with the repo is `config.example.toml`; keep the two
//! in step, since that file is what `README.md` tells operators to copy.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::reconcile::{Budget, budget};

/// Dropbox permits a long-poll timeout in this range (seconds).
const LONGPOLL_TIMEOUT_RANGE: std::ops::RangeInclusive<u64> = 30..=480;

/// The daemon's on-disk configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Local directory kept in sync.
    pub local_root: PathBuf,

    /// Dropbox-side folder to mirror; empty string means the app root.
    #[serde(default)]
    pub remote_root: String,

    /// Dropbox app key. No app secret: dbsync uses OAuth2 PKCE.
    pub app_key: String,

    #[serde(default)]
    pub longpoll: LongpollConfig,

    #[serde(default)]
    pub watcher: WatcherConfig,

    #[serde(default)]
    pub download: DownloadConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LongpollConfig {
    /// Seconds a long-poll request may block before returning `changes: false`.
    #[serde(default = "default_longpoll_timeout")]
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WatcherConfig {
    /// How long local filesystem events are coalesced before uploading.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
}

/// Admission limits for parallel downloads.
///
/// The defaults live in [`crate::reconcile::Budget`], which is where the policy
/// is reasoned about; this struct only carries them in from the file.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadConfig {
    /// How many bytes of downloads may be in flight at once.
    #[serde(default = "default_budget_bytes")]
    pub budget_bytes: u64,

    /// How many downloads are admitted regardless of the byte budget.
    #[serde(default = "default_min_concurrency")]
    pub min_concurrency: usize,

    /// The hard cap on downloads in flight, whatever their size.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
}

impl DownloadConfig {
    /// The limits as the reconciler wants them.
    pub fn budget(&self) -> Budget {
        Budget {
            bytes: self.budget_bytes,
            floor: self.min_concurrency,
            ceiling: self.max_concurrency,
        }
    }
}

fn default_budget_bytes() -> u64 {
    budget::DEFAULT_BYTES
}

fn default_min_concurrency() -> usize {
    budget::DEFAULT_FLOOR
}

fn default_max_concurrency() -> usize {
    budget::DEFAULT_CEILING
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            budget_bytes: default_budget_bytes(),
            min_concurrency: default_min_concurrency(),
            max_concurrency: default_max_concurrency(),
        }
    }
}

fn default_longpoll_timeout() -> u64 {
    300
}

fn default_debounce_ms() -> u64 {
    500
}

impl Default for LongpollConfig {
    fn default() -> Self {
        Self {
            timeout_secs: default_longpoll_timeout(),
        }
    }
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            debounce_ms: default_debounce_ms(),
        }
    }
}

/// Read one `DBSYNC_` override, if it is set and non-empty.
fn env_parse<T: std::str::FromStr>(name: &str) -> Result<Option<T>> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(None);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse()
        .map(Some)
        .map_err(|_| Error::Config(format!("{name} is not a valid number: {raw:?}")))
}

impl Config {
    /// Read and validate a config file.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config: Config =
            toml::from_str(&text).map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        config.apply_env()?;
        config.validate()?;
        Ok(config)
    }

    /// Let `DBSYNC_`-prefixed environment variables override the file.
    ///
    /// Tuning a container's download concurrency should not need the config
    /// file rebuilt or bind-mounted afresh, so the knobs an operator is most
    /// likely to turn are also readable from the environment.
    fn apply_env(&mut self) -> Result<()> {
        if let Some(value) = env_parse("DBSYNC_DOWNLOAD_BUDGET_BYTES")? {
            self.download.budget_bytes = value;
        }
        if let Some(value) = env_parse("DBSYNC_DOWNLOAD_MIN_CONCURRENCY")? {
            self.download.min_concurrency = value;
        }
        if let Some(value) = env_parse("DBSYNC_DOWNLOAD_MAX_CONCURRENCY")? {
            self.download.max_concurrency = value;
        }
        Ok(())
    }

    /// Reject values Dropbox would refuse, so the daemon fails at startup
    /// rather than on its first API call.
    fn validate(&self) -> Result<()> {
        if self.app_key.trim().is_empty() || self.app_key == "your-app-key-here" {
            return Err(Error::Config("app_key is not set".into()));
        }
        if !LONGPOLL_TIMEOUT_RANGE.contains(&self.longpoll.timeout_secs) {
            return Err(Error::Config(format!(
                "longpoll.timeout_secs must be between {} and {}, got {}",
                LONGPOLL_TIMEOUT_RANGE.start(),
                LONGPOLL_TIMEOUT_RANGE.end(),
                self.longpoll.timeout_secs,
            )));
        }
        // A zero budget or an inverted pair would leave the admission gate
        // unable to admit anything, so it is refused here rather than silently
        // clamped: a stalled pull is far harder to diagnose than a startup error.
        if self.download.budget_bytes == 0 {
            return Err(Error::Config(
                "download.budget_bytes must be at least 1".into(),
            ));
        }
        if self.download.min_concurrency == 0 {
            return Err(Error::Config(
                "download.min_concurrency must be at least 1".into(),
            ));
        }
        if self.download.max_concurrency < self.download.min_concurrency {
            return Err(Error::Config(format!(
                "download.max_concurrency ({}) must be at least download.min_concurrency ({})",
                self.download.max_concurrency, self.download.min_concurrency,
            )));
        }
        if !self.remote_root.is_empty() && !self.remote_root.starts_with('/') {
            return Err(Error::Config(
                "remote_root must be empty or begin with '/'".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(text: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, text).unwrap();
        (dir, path)
    }

    const VALID: &str = r#"
        local_root = "/data/Dropbox"
        app_key = "abc123"
    "#;

    #[test]
    fn loads_a_minimal_config_with_defaults() {
        let (_dir, path) = write(VALID);
        let config = Config::load(&path).unwrap();
        assert_eq!(config.app_key, "abc123");
        assert_eq!(config.remote_root, "");
        assert_eq!(config.longpoll.timeout_secs, 300);
        assert_eq!(config.watcher.debounce_ms, 500);
        assert_eq!(config.download.budget(), Budget::default());
    }

    #[test]
    fn rejects_a_zero_byte_download_budget() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            app_key = "abc123"
            [download]
            budget_bytes = 0
        "#,
        );
        assert!(matches!(Config::load(&path), Err(Error::Config(_))));
    }

    #[test]
    fn rejects_a_ceiling_below_the_floor() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            app_key = "abc123"
            [download]
            min_concurrency = 8
            max_concurrency = 2
        "#,
        );
        assert!(matches!(Config::load(&path), Err(Error::Config(_))));
    }

    #[test]
    fn rejects_a_zero_concurrency_floor() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            app_key = "abc123"
            [download]
            min_concurrency = 0
        "#,
        );
        assert!(matches!(Config::load(&path), Err(Error::Config(_))));
    }

    #[test]
    fn a_configured_budget_reaches_the_reconciler() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            app_key = "abc123"
            [download]
            budget_bytes = 1024
            min_concurrency = 2
            max_concurrency = 3
        "#,
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(
            config.download.budget(),
            Budget {
                bytes: 1024,
                floor: 2,
                ceiling: 3,
            }
        );
    }

    /// Env overrides are read at load; a bad one is a startup error, not a
    /// silently ignored string.
    #[test]
    fn an_unparseable_env_override_is_rejected() {
        assert!(matches!(
            env_parse::<u64>("DBSYNC_TEST_NOT_A_NUMBER_FIXTURE"),
            Ok(None)
        ));
        // SAFETY: single-threaded within this test; the name is unique to it.
        unsafe { std::env::set_var("DBSYNC_TEST_NOT_A_NUMBER_FIXTURE", "lots") };
        let parsed = env_parse::<u64>("DBSYNC_TEST_NOT_A_NUMBER_FIXTURE");
        unsafe { std::env::remove_var("DBSYNC_TEST_NOT_A_NUMBER_FIXTURE") };
        assert!(matches!(parsed, Err(Error::Config(_))));
    }

    #[test]
    fn rejects_the_placeholder_app_key() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            app_key = "your-app-key-here"
        "#,
        );
        assert!(matches!(Config::load(&path), Err(Error::Config(_))));
    }

    #[test]
    fn rejects_a_longpoll_timeout_dropbox_would_refuse() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            app_key = "abc123"
            [longpoll]
            timeout_secs = 5
        "#,
        );
        assert!(matches!(Config::load(&path), Err(Error::Config(_))));
    }

    #[test]
    fn rejects_a_relative_remote_root() {
        let (_dir, path) = write(
            r#"
            local_root = "/data/Dropbox"
            remote_root = "Photos"
            app_key = "abc123"
        "#,
        );
        assert!(matches!(Config::load(&path), Err(Error::Config(_))));
    }
}
