//! Persistence of the long-lived refresh token.
//!
//! The token is the account credential, so it is kept outside the repo — under
//! the XDG data directory — and written with owner-only permissions. In the
//! container, `compose.yaml` mounts a named volume at that path so it survives
//! rebuilds and never lands in the image.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::fsutil;

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
        Ok(Self::at(fsutil::data_dir()?.join("credentials.json")))
    }

    /// Where this store reads and writes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load stored credentials, or [`Error::NotAuthenticated`] if absent.
    pub fn load(&self) -> Result<StoredCredentials> {
        let Some(text) = fsutil::read_optional(&self.path)? else {
            return Err(Error::NotAuthenticated);
        };
        serde_json::from_str(&text)
            .map_err(|e| Error::Config(format!("{}: {e}", self.path.display())))
    }

    /// Write credentials with owner-only permissions, replacing any existing
    /// file atomically so a crash cannot leave a truncated token behind.
    pub fn save(&self, credentials: &StoredCredentials) -> Result<()> {
        fsutil::write_json_atomically(
            &self.path,
            credentials,
            "credentials",
            Some(TOKEN_FILE_MODE),
        )
    }

    /// True when credentials are present.
    pub fn exists(&self) -> bool {
        self.path.exists()
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

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
