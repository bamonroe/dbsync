//! Applying one remote entry to the local disk.
//!
//! Each function here handles a single change and updates the state to match
//! what it just did, so the state never claims a file the disk does not have.

use std::path::Path;

use crate::api::{RemoteEntry, RemoteFile};
use crate::error::{Error, Result};
use crate::state::{SyncState, entry_for_local_file, key_for};

use super::conflict;
use super::paths::PathMapper;
use super::source::RemoteSource;

/// What applying one entry actually did — used for logging and for the tests
/// that pin the skip behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The file was downloaded and put in place.
    Downloaded,
    /// Already correct locally: a remote echo of what we already have.
    AlreadyCurrent,
    /// A directory was created.
    Directory,
    /// The path was removed locally.
    Deleted,
    /// Nothing to delete — the path was already gone.
    NothingToDelete,
    /// The local file had diverged, so it was kept as a conflicted copy and
    /// the remote version was downloaded over the original.
    Conflicted,
}

/// Apply one entry from a listing or change stream.
pub async fn apply_entry<S: RemoteSource>(
    source: &S,
    paths: &PathMapper,
    state: &mut SyncState,
    entry: &RemoteEntry,
) -> Result<Applied> {
    let local = paths.to_local(entry.display_path())?;
    match entry {
        RemoteEntry::File(file) => apply_file(source, state, file, &local).await,
        RemoteEntry::Folder(_) => {
            tokio::fs::create_dir_all(&local).await?;
            Ok(Applied::Directory)
        }
        RemoteEntry::Deleted(_) => apply_delete(state, entry.display_path(), &local).await,
    }
}

/// Download a file unless we already hold exactly that revision.
async fn apply_file<S: RemoteSource>(
    source: &S,
    state: &mut SyncState,
    file: &RemoteFile,
    local: &Path,
) -> Result<Applied> {
    if is_current(state, file, local) {
        return Ok(Applied::AlreadyCurrent);
    }
    // Both sides moved: the remote version is about to land on this path, so
    // the local bytes have to be set aside first or they are gone for good.
    let conflicted = conflict::has_local_edit(
        state,
        &file.path_display,
        local,
        file.content_hash.as_deref(),
    )?;
    if conflicted {
        conflict::preserve(local).await?;
    }
    source.download_to(&file.path_display, local).await?;
    // Re-describe from disk rather than from the metadata: the hash and mtime
    // must be the ones a local scan will actually see, or the very next scan
    // would read this file as a local edit and upload it straight back.
    let entry = entry_for_local_file(local, &file.path_display, &file.rev)?;
    state.insert(entry);
    Ok(match conflicted {
        true => Applied::Conflicted,
        false => Applied::Downloaded,
    })
}

/// Would downloading this file be a no-op?
///
/// The `rev` alone is not enough — the state can claim a file that a user has
/// since deleted locally — so the file must also still be on disk.
fn is_current(state: &SyncState, file: &RemoteFile, local: &Path) -> bool {
    let Some(known) = state.get(&file.path_display) else {
        return false;
    };
    known.rev == file.rev && local.exists()
}

/// Remove a path the remote says is gone, along with anything under it.
async fn apply_delete(state: &mut SyncState, display_path: &str, local: &Path) -> Result<Applied> {
    forget_subtree(state, display_path);
    match tokio::fs::symlink_metadata(local).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Applied::NothingToDelete),
        Err(error) => Err(Error::ReadFile {
            path: local.to_path_buf(),
            source: error,
        }),
        Ok(metadata) => {
            match metadata.is_dir() {
                true => tokio::fs::remove_dir_all(local).await?,
                false => tokio::fs::remove_file(local).await?,
            }
            Ok(Applied::Deleted)
        }
    }
}

/// Drop the path and every descendant from the state.
///
/// A tombstone for a folder is the only notice we get that its contents are
/// gone too; Dropbox does not send one tombstone per child.
fn forget_subtree(state: &mut SyncState, display_path: &str) {
    let key = key_for(display_path);
    let prefix = format!("{key}/");
    let doomed: Vec<String> = state
        .entries()
        .map(|entry| key_for(&entry.display_path))
        .filter(|candidate| *candidate == key || candidate.starts_with(&prefix))
        .collect();
    for path in doomed {
        state.remove(&path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::testing::FakeRemote;
    use crate::state::SyncEntry;

    fn file(path: &str, rev: &str) -> RemoteEntry {
        RemoteEntry::File(RemoteFile {
            path_lower: path.to_lowercase(),
            path_display: path.to_string(),
            rev: rev.to_string(),
            size: 0,
            content_hash: None,
        })
    }

    fn deleted(path: &str) -> RemoteEntry {
        RemoteEntry::Deleted(crate::api::RemoteDeleted {
            path_lower: path.to_lowercase(),
            path_display: Some(path.to_string()),
        })
    }

    fn folder(path: &str) -> RemoteEntry {
        RemoteEntry::Folder(crate::api::RemoteFolder {
            path_lower: path.to_lowercase(),
            path_display: path.to_string(),
        })
    }

    struct Fixture {
        dir: tempfile::TempDir,
        remote: FakeRemote,
        state: SyncState,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().unwrap(),
                remote: FakeRemote::new(),
                state: SyncState::new(),
            }
        }

        fn paths(&self) -> PathMapper {
            PathMapper::new(self.dir.path(), "")
        }

        async fn apply(&mut self, entry: &RemoteEntry) -> Result<Applied> {
            let paths = self.paths();
            apply_entry(&self.remote, &paths, &mut self.state, entry).await
        }

        fn local(&self, relative: &str) -> std::path::PathBuf {
            self.dir.path().join(relative)
        }
    }

    /// The download-side conflict: the incoming version would overwrite local
    /// edits, so the local bytes are kept beside it.
    #[tokio::test]
    async fn a_locally_edited_file_is_kept_as_a_conflicted_copy() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"theirs");
        std::fs::write(fixture.local("a.txt"), b"mine").unwrap();

        assert_eq!(
            fixture.apply(&file("/a.txt", "r2")).await.unwrap(),
            Applied::Conflicted
        );
        assert_eq!(std::fs::read(fixture.local("a.txt")).unwrap(), b"theirs");
        assert_eq!(
            std::fs::read(fixture.local("a (conflicted copy).txt")).unwrap(),
            b"mine"
        );
    }

    /// A file that still matches the state has no local edit to protect, so it
    /// is a plain update and no copy is made.
    #[tokio::test]
    async fn an_untouched_file_is_updated_without_a_copy() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"first");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();

        fixture.remote.put("/a.txt", b"second");
        assert_eq!(
            fixture.apply(&file("/a.txt", "r2")).await.unwrap(),
            Applied::Downloaded
        );
        assert!(!fixture.local("a (conflicted copy).txt").exists());
    }

    #[tokio::test]
    async fn a_new_file_is_downloaded_and_recorded() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");

        assert_eq!(
            fixture.apply(&file("/a.txt", "r1")).await.unwrap(),
            Applied::Downloaded
        );
        assert_eq!(std::fs::read(fixture.local("a.txt")).unwrap(), b"hello");
        assert_eq!(fixture.state.get("/a.txt").unwrap().rev, "r1");
    }

    /// The state must describe the file as it landed on disk, or the local
    /// watcher would read our own download as a local edit and upload it back.
    #[tokio::test]
    async fn the_recorded_entry_matches_the_downloaded_file() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();

        let entry = fixture.state.get("/a.txt").unwrap();
        let metadata = std::fs::metadata(fixture.local("a.txt")).unwrap();
        assert!(entry.metadata_matches(metadata.len(), metadata.modified().unwrap()));
        assert_eq!(entry.content_hash, crate::state::hash::hash_bytes(b"hello"));
    }

    /// An echo of a revision we already hold must not re-download.
    #[tokio::test]
    async fn a_known_revision_is_skipped() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();

        assert_eq!(
            fixture.apply(&file("/a.txt", "r1")).await.unwrap(),
            Applied::AlreadyCurrent
        );
        assert_eq!(fixture.remote.downloads(), 1);
    }

    #[tokio::test]
    async fn a_new_revision_overwrites_the_old_content() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();

        fixture.remote.put("/a.txt", b"goodbye");
        assert_eq!(
            fixture.apply(&file("/a.txt", "r2")).await.unwrap(),
            Applied::Downloaded
        );
        assert_eq!(std::fs::read(fixture.local("a.txt")).unwrap(), b"goodbye");
        assert_eq!(fixture.state.get("/a.txt").unwrap().rev, "r2");
    }

    /// If the user deleted the file locally, the same `rev` must still be
    /// restored rather than skipped.
    #[tokio::test]
    async fn a_locally_missing_file_is_re_downloaded_at_the_same_rev() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();
        std::fs::remove_file(fixture.local("a.txt")).unwrap();

        assert_eq!(
            fixture.apply(&file("/a.txt", "r1")).await.unwrap(),
            Applied::Downloaded
        );
        assert!(fixture.local("a.txt").exists());
    }

    #[tokio::test]
    async fn a_nested_file_creates_its_parent_directories() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/deep/er/a.txt", b"hi");

        fixture.apply(&file("/deep/er/a.txt", "r1")).await.unwrap();
        assert!(fixture.local("deep/er/a.txt").exists());
    }

    #[tokio::test]
    async fn a_folder_entry_creates_the_directory() {
        let mut fixture = Fixture::new();
        assert_eq!(
            fixture.apply(&folder("/photos")).await.unwrap(),
            Applied::Directory
        );
        assert!(fixture.local("photos").is_dir());
    }

    #[tokio::test]
    async fn a_tombstone_removes_the_file_and_forgets_it() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();

        assert_eq!(
            fixture.apply(&deleted("/a.txt")).await.unwrap(),
            Applied::Deleted
        );
        assert!(!fixture.local("a.txt").exists());
        assert!(fixture.state.get("/a.txt").is_none());
    }

    /// One tombstone arrives for a deleted folder, not one per child, so the
    /// whole subtree must go — on disk and in the state.
    #[tokio::test]
    async fn a_folder_tombstone_removes_the_whole_subtree() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/dir/a.txt", b"a");
        fixture.remote.put("/dir/sub/b.txt", b"b");
        fixture.apply(&file("/dir/a.txt", "r1")).await.unwrap();
        fixture.apply(&file("/dir/sub/b.txt", "r1")).await.unwrap();

        fixture.apply(&deleted("/dir")).await.unwrap();
        assert!(!fixture.local("dir").exists());
        assert_eq!(fixture.state.len(), 0);
    }

    /// A sibling whose name merely starts with the deleted folder's name must
    /// survive.
    #[tokio::test]
    async fn a_delete_does_not_take_similarly_named_siblings() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/dir/a.txt", b"a");
        fixture.remote.put("/dirty.txt", b"b");
        fixture.apply(&file("/dir/a.txt", "r1")).await.unwrap();
        fixture.apply(&file("/dirty.txt", "r1")).await.unwrap();

        fixture.apply(&deleted("/dir")).await.unwrap();
        assert!(fixture.local("dirty.txt").exists());
        assert!(fixture.state.get("/dirty.txt").is_some());
    }

    /// Deletes are routinely reported for paths we never had; that is not an
    /// error.
    #[tokio::test]
    async fn deleting_something_we_never_had_is_fine() {
        let mut fixture = Fixture::new();
        assert_eq!(
            fixture.apply(&deleted("/never.txt")).await.unwrap(),
            Applied::NothingToDelete
        );
    }

    /// The security case, end to end: a hostile path must not write outside
    /// the sync root.
    #[tokio::test]
    async fn a_traversal_path_is_refused() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/../escape.txt", b"x");
        assert!(fixture.apply(&file("/../escape.txt", "r1")).await.is_err());
    }

    #[tokio::test]
    async fn a_file_entry_for_an_unknown_path_reports_the_download_failure() {
        let mut fixture = Fixture::new();
        let mut state = SyncState::new();
        state.insert(SyncEntry {
            rev: "r0".into(),
            content_hash: "h".into(),
            mtime_nanos: 0,
            size: 1,
            display_path: "/missing.txt".into(),
        });
        fixture.state = state;
        // Same rev, but the file is not on disk, so this attempts a download
        // the fake remote cannot serve.
        assert!(fixture.apply(&file("/missing.txt", "r0")).await.is_err());
    }
}
