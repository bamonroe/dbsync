//! The local sync state database.
//!
//! Records what dbsync last agreed with Dropbox: for each file its `rev`,
//! content hash, local mtime and size, plus the folder cursor marking our
//! position in the remote change stream. This is what makes a restart cheap and
//! an echo of our own upload recognisable.
//!
//! Two invariants from `docs/architecture.md` live here:
//!
//! - **Writes are atomic.** The state is replaced by rename, so a crash leaves
//!   either the old file or the new one — never a torn one that disagrees with
//!   the disk.
//! - **Content identity is the Dropbox content hash**, not mtime; see [`hash`].
//!
//! Per the same doc, [`crate::reconcile`] is the only component that should
//! write this.

mod db;
mod entry;
pub mod failures;
pub mod hash;
pub mod journal;
pub mod requests;

pub use db::{StateDb, SyncState, key_for};
pub use entry::{SyncEntry, from_nanos, to_nanos};
pub use failures::{Direction, Failure, FailureKind};
pub use requests::{RetryQueue, RetryRequest};

use std::path::Path;

use crate::error::{Error, Result};

/// Build an entry describing a local file as it stands right now.
///
/// Hashes the file, so call it only when the cheap metadata check in
/// [`SyncEntry::metadata_matches`] has already suggested something changed.
pub fn entry_for_local_file(
    local_path: &Path,
    display_path: impl Into<String>,
    rev: impl Into<String>,
) -> Result<SyncEntry> {
    let metadata = std::fs::metadata(local_path).map_err(|source| Error::ReadFile {
        path: local_path.to_path_buf(),
        source,
    })?;
    let mtime = metadata.modified().map_err(|source| Error::ReadFile {
        path: local_path.to_path_buf(),
        source,
    })?;
    Ok(SyncEntry {
        rev: rev.into(),
        content_hash: hash::hash_file(local_path)?,
        mtime_nanos: to_nanos(mtime),
        size: metadata.len(),
        display_path: display_path.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_describes_the_file_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        std::fs::write(&path, b"hello").unwrap();

        let entry = entry_for_local_file(&path, "/note.txt", "r1").unwrap();
        assert_eq!(entry.size, 5);
        assert_eq!(entry.content_hash, hash::hash_bytes(b"hello"));
        assert_eq!(entry.display_path, "/note.txt");
        assert_eq!(entry.rev, "r1");
    }

    /// The freshly built entry must agree with the file it just described,
    /// otherwise every scan would look like a change.
    #[test]
    fn a_fresh_entry_matches_its_own_files_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        std::fs::write(&path, b"hello").unwrap();

        let entry = entry_for_local_file(&path, "/note.txt", "r1").unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(entry.metadata_matches(metadata.len(), metadata.modified().unwrap()));
    }

    #[test]
    fn a_missing_file_is_a_read_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("gone.txt");
        assert!(matches!(
            entry_for_local_file(&missing, "/gone.txt", "r1"),
            Err(Error::ReadFile { .. })
        ));
    }
}
