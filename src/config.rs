//! Loading and validating `config.toml`.
//!
//! The example that ships with the repo is `config.example.toml`; keep the two
//! in step, since that file is what `README.md` tells operators to copy.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};

/// Dropbox permits a long-poll timeout in this range (seconds).
const LONGPOLL_TIMEOUT_RANGE: std::ops::RangeInclusive<u64> = 30..=480;

/// The daemon's on-disk configuration.
#[derive(Debug, Clone, Deserialize)]
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct LongpollConfig {
    /// Seconds a long-poll request may block before returning `changes: false`.
    #[serde(default = "default_longpoll_timeout")]
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WatcherConfig {
    /// How long local filesystem events are coalesced before uploading.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
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

impl Config {
    /// Read and validate a config file.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Config =
            toml::from_str(&text).map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
        config.validate()?;
        Ok(config)
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
