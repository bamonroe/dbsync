//! Persistence of the long-lived refresh token.
//!
//! The token is the account credential, so it is kept outside the repo — under
//! the XDG data directory — and written with owner-only permissions. In the
//! container, `compose.yaml` mounts a named volume at that path so it survives
//! rebuilds and never lands in the image.

use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Owner read/write only. The refresh token is a bearer credential.
const TOKEN_FILE_MODE: u32 = 0o600;

/// What `dbsync auth login` persists.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredCredentials {
    /// The long-lived refresh token.
    pub refresh_token: String,
    /// The account this token belongs to, for `auth status`.
    #[serde(default)]
    pub account_id: Option<String>,
}

/// Reads and writes [`StoredCredentials`] at a fixed path.
pub struct TokenStore {
    path: PathBuf,
}

impl TokenStore {
    /// A store rooted at an explicit file path.
    pub fn at(path: PathBuf) -> Self {
        Self { path }
    }

    /// The default location: `$XDG_DATA_HOME/dbsync/credentials.json`.
    pub fn default_location() -> Result<Self> {
        let dirs = directories::ProjectDirs::from("", "", "dbsync")
            .ok_or_else(|| Error::Config("cannot determine a home directory".into()))?;
        Ok(Self::at(dirs.data_dir().join("credentials.json")))
    }

    /// Where this store reads and writes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load stored credentials, or [`Error::NotAuthenticated`] if absent.
    pub fn load(&self) -> Result<StoredCredentials> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotAuthenticated);
            }
            Err(source) => {
                return Err(Error::ReadFile {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        serde_json::from_str(&text)
            .map_err(|e| Error::Config(format!("{}: {e}", self.path.display())))
    }

    /// Write credentials with owner-only permissions, replacing any existing
    /// file atomically so a crash cannot leave a truncated token behind.
    pub fn save(&self, credentials: &StoredCredentials) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
        let temp = self.path.with_extension("tmp");
        let json = serde_json::to_string_pretty(credentials)
            .map_err(|e| Error::Config(format!("cannot serialise credentials: {e}")))?;

        // Create with the restrictive mode from the start: writing first and
        // chmod-ing after would leave the token world-readable in between.
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(TOKEN_FILE_MODE)
                .open(&temp)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&temp, &self.path)?;
        Ok(())
    }

    /// True when credentials are present.
    pub fn exists(&self) -> bool {
        self.path.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, TokenStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::at(dir.path().join("nested").join("credentials.json"));
        (dir, store)
    }

    fn creds() -> StoredCredentials {
        StoredCredentials {
            refresh_token: "rt-secret".into(),
            account_id: Some("dbid:abc".into()),
        }
    }

    #[test]
    fn a_missing_file_reads_as_not_authenticated() {
        let (_dir, store) = store();
        assert!(matches!(store.load(), Err(Error::NotAuthenticated)));
        assert!(!store.exists());
    }

    #[test]
    fn saved_credentials_round_trip() {
        let (_dir, store) = store();
        store.save(&creds()).unwrap();
        assert!(store.exists());
        assert_eq!(store.load().unwrap(), creds());
    }

    /// The refresh token is a bearer credential; it must not be group- or
    /// world-readable.
    #[test]
    fn the_token_file_is_owner_only() {
        let (_dir, store) = store();
        store.save(&creds()).unwrap();
        let mode = std::fs::metadata(store.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, TOKEN_FILE_MODE);
    }

    #[test]
    fn saving_twice_replaces_rather_than_appends() {
        let (_dir, store) = store();
        store.save(&creds()).unwrap();
        let mut updated = creds();
        updated.refresh_token = "rt-second".into();
        store.save(&updated).unwrap();
        assert_eq!(store.load().unwrap().refresh_token, "rt-second");
    }

    /// A save must not leave its temporary file lying around.
    #[test]
    fn no_temp_file_survives_a_save() {
        let (_dir, store) = store();
        store.save(&creds()).unwrap();
        assert!(!store.path().with_extension("tmp").exists());
    }
}
