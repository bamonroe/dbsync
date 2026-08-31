//! The crate-wide error type.
//!
//! Kept separate from the modules that raise these so every layer can name the
//! same failure without a dependency cycle.

use std::path::PathBuf;

/// Errors surfaced by any dbsync component.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("could not read {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("not linked to a Dropbox account — run `dbsync auth login`")]
    NotAuthenticated,

    /// The access token was rejected. The caller should refresh and retry once.
    #[error("Dropbox rejected our credentials")]
    Unauthorized,

    /// Dropbox invalidated our folder cursor. Not exceptional: the caller must
    /// drop the cursor, re-list the folder, and reconcile. See
    /// `docs/architecture.md`.
    #[error("folder cursor was reset by Dropbox")]
    CursorReset,

    /// Dropbox refused a write because the revision we named is no longer the
    /// current one: someone else edited the file. The caller must keep both
    /// versions — see `src/reconcile/conflict.rs`.
    #[error("Dropbox refused the write: the file changed remotely")]
    Conflict,

    /// Dropbox asked us to slow down; wait this long before retrying.
    #[error("rate limited; retry after {0}s")]
    RateLimited(u64),

    #[error("Dropbox API error ({status}): {message}")]
    Api { status: u16, message: String },

    /// Work handed to the blocking pool never came back — it panicked, or the
    /// runtime is shutting down. See [`crate::blocking`].
    #[error("background work failed: {0}")]
    Blocking(String),

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
