//! Pushing one local path up to Dropbox.
//!
//! The hard part is not the upload; it is deciding whether to upload at all.
//! Every remote change we apply writes a file locally, and the watcher sees
//! that write — so without a check, each download would bounce straight back up
//! as an edit. The state database is what breaks that loop.

use std::path::Path;

use crate::api::WriteMode;
use crate::error::Result;
use crate::state::{SyncEntry, SyncState, entry_for_local_file, hash};

use super::paths::PathMapper;
use super::sink::RemoteSink;

/// What pushing one path actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    /// The file was sent to Dropbox.
    Uploaded,
    /// The local file is gone, so the remote copy was deleted.
    Deleted,
    /// Local and remote already agree.
    Unchanged,
    /// Not a file we sync — a directory, or something outside the root.
    Ignored,
}

/// Push whatever is at `local` now: upload, delete, or decide it is unchanged.
pub async fn push_path<S: RemoteSink>(
    sink: &S,
    paths: &PathMapper,
    state: &mut SyncState,
    local: &Path,
) -> Result<Pushed> {
    let remote = paths.to_remote(local)?;
    let Some(metadata) = read_metadata(local).await? else {
        return delete_remote(sink, state, &remote).await;
    };
    if metadata.is_dir() {
        // Directories need no upload of their own: Dropbox creates them
        // implicitly when a file inside one is uploaded.
        return Ok(Pushed::Ignored);
    }
    upload_if_changed(sink, state, &remote, local, &metadata).await
}

/// The local metadata, or `None` if the path no longer exists.
async fn read_metadata(local: &Path) -> Result<Option<std::fs::Metadata>> {
    match tokio::fs::symlink_metadata(local).await {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Delete the remote copy of a file that has gone from disk.
async fn delete_remote<S: RemoteSink>(
    sink: &S,
    state: &mut SyncState,
    remote: &str,
) -> Result<Pushed> {
    // Never delete remotely for a path we were not tracking: an unknown local
    // path that does not exist is nothing at all, not a deletion.
    if state.get(remote).is_none() {
        return Ok(Pushed::Ignored);
    }
    sink.delete(remote).await?;
    state.remove(remote);
    Ok(Pushed::Deleted)
}

/// Upload the file unless the state says Dropbox already has these bytes.
async fn upload_if_changed<S: RemoteSink>(
    sink: &S,
    state: &mut SyncState,
    remote: &str,
    local: &Path,
    metadata: &std::fs::Metadata,
) -> Result<Pushed> {
    let known = state.get(remote).cloned();
    if let Some(entry) = &known
        && unchanged(entry, metadata)?
    {
        return Ok(Pushed::Unchanged);
    }
    // The cheap check failed, so hash: an editor that rewrites a file byte for
    // byte changes its mtime without changing its content, and that must not
    // become an upload.
    let content_hash = hash::hash_file(local)?;
    if let Some(entry) = &known
        && entry.content_hash == content_hash
    {
        // Same bytes, new mtime: re-record so the next scan is cheap again.
        state.insert(refreshed(entry, metadata));
        return Ok(Pushed::Unchanged);
    }

    let mode = match &known {
        // Naming the revision we expect is what stops us clobbering a remote
        // edit we have not seen yet; Dropbox refuses instead.
        Some(entry) => WriteMode::Update(entry.rev.clone()),
        None => WriteMode::Add,
    };
    let uploaded = sink.upload(remote, local, &mode).await?;
    state.insert(entry_for_local_file(
        local,
        &uploaded.path_display,
        &uploaded.rev,
    )?);
    Ok(Pushed::Uploaded)
}

/// Does the cheap metadata check say nothing happened?
fn unchanged(entry: &SyncEntry, metadata: &std::fs::Metadata) -> Result<bool> {
    Ok(entry.metadata_matches(metadata.len(), metadata.modified()?))
}

/// The same entry, re-stamped with the file's current metadata.
fn refreshed(entry: &SyncEntry, metadata: &std::fs::Metadata) -> SyncEntry {
    SyncEntry {
        size: metadata.len(),
        mtime_nanos: metadata
            .modified()
            .map(crate::state::to_nanos)
            .unwrap_or(entry.mtime_nanos),
        ..entry.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::testing::FakeRemote;

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

        fn write(&self, relative: &str, content: &[u8]) -> std::path::PathBuf {
            let path = self.dir.path().join(relative);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&path, content).unwrap();
            path
        }

        async fn push(&mut self, relative: &str) -> Result<Pushed> {
            let paths = self.paths();
            let local = self.dir.path().join(relative);
            push_path(&self.remote, &paths, &mut self.state, &local).await
        }
    }

    #[tokio::test]
    async fn a_new_file_is_uploaded_and_recorded() {
        let mut fixture = Fixture::new();
        fixture.write("a.txt", b"hello");

        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Uploaded);
        assert_eq!(fixture.remote.content("/a.txt").unwrap(), b"hello");
        assert!(fixture.state.get("/a.txt").is_some());
    }

    /// A new file must not claim a revision it has no right to.
    #[tokio::test]
    async fn a_new_file_uploads_with_add() {
        let mut fixture = Fixture::new();
        fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();

        assert_eq!(fixture.remote.modes(), vec![WriteMode::Add]);
    }

    /// An edit names the revision it believes it is replacing, so a concurrent
    /// remote edit is refused rather than overwritten.
    #[tokio::test]
    async fn an_edit_uploads_with_the_revision_it_knows() {
        let mut fixture = Fixture::new();
        fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();
        let rev = fixture.state.get("/a.txt").unwrap().rev.clone();

        fixture.write("a.txt", b"hello again");
        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Uploaded);
        assert_eq!(fixture.remote.modes()[1], WriteMode::Update(rev));
    }

    /// The loop-breaker: a file we just recorded must not be re-uploaded.
    #[tokio::test]
    async fn an_unchanged_file_is_not_uploaded_again() {
        let mut fixture = Fixture::new();
        fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();

        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Unchanged);
        assert_eq!(fixture.remote.uploads(), 1);
    }

    /// A rewrite with identical bytes changes the mtime only; hashing catches
    /// that and skips the upload.
    #[tokio::test]
    async fn identical_content_with_a_new_mtime_is_not_uploaded() {
        let mut fixture = Fixture::new();
        let path = fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();

        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        std::fs::File::open(&path)
            .unwrap()
            .set_modified(later)
            .unwrap();

        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Unchanged);
        assert_eq!(fixture.remote.uploads(), 1);
        // The state is re-stamped, so the next check is cheap again.
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(
            fixture
                .state
                .get("/a.txt")
                .unwrap()
                .metadata_matches(metadata.len(), metadata.modified().unwrap())
        );
    }

    #[tokio::test]
    async fn a_deleted_file_deletes_the_remote_copy() {
        let mut fixture = Fixture::new();
        let path = fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Deleted);
        assert!(fixture.remote.content("/a.txt").is_none());
        assert!(fixture.state.get("/a.txt").is_none());
    }

    /// A path we never tracked and that does not exist is nothing — deleting
    /// remotely on that basis would be destructive.
    #[tokio::test]
    async fn an_unknown_missing_path_deletes_nothing() {
        let mut fixture = Fixture::new();
        assert_eq!(fixture.push("never.txt").await.unwrap(), Pushed::Ignored);
        assert_eq!(fixture.remote.deletes(), 0);
    }

    #[tokio::test]
    async fn a_directory_is_not_uploaded() {
        let mut fixture = Fixture::new();
        std::fs::create_dir(fixture.dir.path().join("sub")).unwrap();
        assert_eq!(fixture.push("sub").await.unwrap(), Pushed::Ignored);
        assert_eq!(fixture.remote.uploads(), 0);
    }

    #[tokio::test]
    async fn a_nested_file_keeps_its_remote_path() {
        let mut fixture = Fixture::new();
        fixture.write("deep/er/a.txt", b"hi");
        fixture.push("deep/er/a.txt").await.unwrap();
        assert!(fixture.remote.content("/deep/er/a.txt").is_some());
    }

    /// Anything outside the sync root has no remote path at all.
    #[tokio::test]
    async fn a_path_outside_the_root_is_refused() {
        let mut fixture = Fixture::new();
        let paths = fixture.paths();
        let outside = Path::new("/etc/passwd");
        assert!(
            push_path(&fixture.remote, &paths, &mut fixture.state, outside)
                .await
                .is_err()
        );
    }
}
