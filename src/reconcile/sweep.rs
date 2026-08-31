//! Clearing leftover partial downloads out of the synced tree.
//!
//! [`crate::api::download_to`] writes to a `.dbsync-partial` sibling and
//! removes it if the transfer errors — but a hard kill never reaches that
//! cleanup, so the scratch file survives. Nothing else looks at it: the
//! watcher filters partials out, and sync never sees them. They just
//! accumulate, invisible, one per interrupted download.

use std::path::Path;

use crate::error::Result;

/// Delete every leftover partial download under `root`, returning how many.
///
/// **Only safe while no download is in flight.** A partial being written right
/// now is indistinguishable from an abandoned one, so this belongs at startup,
/// before the first pull — which is also when it is most useful.
///
/// Walks iteratively rather than recursively: a deep tree should not be able
/// to overflow the stack, and this runs against whatever the user has synced.
///
/// The walk itself runs on the blocking pool — it stats every file in the
/// synced tree, which on a large account is a long time to hold a runtime
/// thread. See [`crate::blocking`].
pub async fn partial_downloads(root: &Path) -> Result<usize> {
    let root = root.to_path_buf();
    crate::blocking::run(move || sweep_tree(&root)).await
}

/// The walk itself, blocking.
fn sweep_tree(root: &Path) -> Result<usize> {
    let mut removed = 0;
    let mut pending = vec![root.to_path_buf()];

    while let Some(directory) = pending.pop() {
        // An unreadable directory is not worth failing startup over; the sync
        // that follows will report anything that actually matters.
        let Ok(entries) = std::fs::read_dir(&directory) else {
            tracing::debug!(path = %directory.display(), "could not read directory while sweeping");
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // `file_type` here does not follow symlinks, so a link pointing
            // out of the tree is never descended into or deleted through.
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => pending.push(path),
                Ok(kind) if kind.is_file() => removed += remove_if_partial(&path),
                _ => {}
            }
        }
    }
    Ok(removed)
}

/// Delete `path` if it is a partial download with nothing worth resuming.
/// Returns 1 if it went.
///
/// A chunked partial that still has its chunk map beside it is *kept*: it is
/// exactly the interrupted large download that resume exists for, and sweeping
/// it would restart a many-gigabyte fetch from byte zero on every daemon
/// restart. The map is trusted, not the length — see [`crate::api::download`].
fn remove_if_partial(path: &Path) -> usize {
    if !crate::api::is_partial(path) || crate::api::is_resumable_partial(path) {
        return 0;
    }
    match crate::fsutil::remove_if_present(path) {
        Ok(true) => {
            tracing::debug!(path = %path.display(), "removed leftover partial download");
            1
        }
        Ok(false) => 0,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "could not remove partial download");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a tree and return its root.
    fn tree(files: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for file in files {
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"x").unwrap();
        }
        dir
    }

    #[tokio::test]
    async fn a_leftover_partial_is_removed() {
        let dir = tree(&["a.txt.dbsync-partial"]);
        assert_eq!(partial_downloads(dir.path()).await.unwrap(), 1);
        assert!(!dir.path().join("a.txt.dbsync-partial").exists());
    }

    /// The whole point is that real files are untouched.
    #[tokio::test]
    async fn real_files_survive_the_sweep() {
        let dir = tree(&["a.txt", "notes.md", "b.dbsync-partial.txt"]);
        assert_eq!(partial_downloads(dir.path()).await.unwrap(), 0);
        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.dbsync-partial.txt").exists());
    }

    #[tokio::test]
    async fn partials_are_found_at_every_depth() {
        let dir = tree(&[
            "top.dbsync-partial",
            "one/mid.dbsync-partial",
            "one/two/three/deep.dbsync-partial",
            "one/two/keep.txt",
        ]);
        assert_eq!(partial_downloads(dir.path()).await.unwrap(), 3);
        assert!(dir.path().join("one/two/keep.txt").exists());
    }

    /// An interrupted chunked download — a partial with its chunk map beside
    /// it — is the very thing resume exists for; the sweep must not eat it.
    #[tokio::test]
    async fn a_resumable_chunked_partial_survives_the_sweep() {
        let dir = tree(&[
            "big.iso.r1.dbsync-partial",
            "big.iso.r1.dbsync-partial-map",
            "small.txt.r1.dbsync-partial",
        ]);
        assert_eq!(partial_downloads(dir.path()).await.unwrap(), 1);
        assert!(dir.path().join("big.iso.r1.dbsync-partial").exists());
        assert!(dir.path().join("big.iso.r1.dbsync-partial-map").exists());
        assert!(!dir.path().join("small.txt.r1.dbsync-partial").exists());
    }

    /// A map whose partial is gone carries no progress; it goes.
    #[tokio::test]
    async fn an_orphaned_chunk_map_is_removed() {
        let dir = tree(&["big.iso.r1.dbsync-partial-map"]);
        assert_eq!(partial_downloads(dir.path()).await.unwrap(), 1);
        assert!(!dir.path().join("big.iso.r1.dbsync-partial-map").exists());
    }

    #[tokio::test]
    async fn an_empty_tree_sweeps_nothing() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(partial_downloads(dir.path()).await.unwrap(), 0);
    }

    /// A missing root is normal on a first run and must not be an error.
    #[tokio::test]
    async fn a_missing_root_is_not_an_error() {
        assert_eq!(
            partial_downloads(Path::new("/tmp/dbsync-nonexistent-root"))
                .await
                .unwrap(),
            0
        );
    }
}
