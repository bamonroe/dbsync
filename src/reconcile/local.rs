//! Pushing one local path up to Dropbox.
//!
//! The hard part is not the upload; it is deciding whether to upload at all.
//! Every remote change we apply writes a file locally, and the watcher sees
//! that write — so without a check, each download would bounce straight back up
//! as an edit. The state database is what breaks that loop.

use std::path::Path;

use crate::api::WriteMode;
use crate::error::{Error, Result};
use crate::state::{SyncEntry, SyncState, entry_for_local_file_off_thread, hash};

use super::conflict;
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
    /// Dropbox refused the write because the file moved remotely, so the local
    /// version was kept as a conflicted copy and uploaded under that name.
    Conflicted,
}

/// Push whatever is at `local` now: upload, delete, or decide it is unchanged.
/// The remote path this local file belongs to.
///
/// Normally the on-disk name *is* the remote name, but a name too long for the
/// filesystem was shortened on the way down, and the shortened name no longer
/// says what the original was. Uploading under it would create a second,
/// wrongly-named remote file, so the recorded alias wins where there is one.
fn remote_path_of(paths: &PathMapper, state: &SyncState, local: &Path) -> Result<String> {
    if let Ok(relative) = paths.relative_key(local)
        && let Some(alias) = state.alias_for(&relative)
    {
        return Ok(alias.to_string());
    }
    // A file *created* inside a shortened folder has no alias of its own, but
    // an enclosing folder does: graft the new components onto the nearest
    // aliased ancestor. Mapping the whole path verbatim instead would upload
    // under the fingerprint-mangled folder name and mint a duplicate,
    // wrongly-named remote tree.
    if let Some(remote) = grafted_onto_ancestor(paths, state, local) {
        return Ok(remote);
    }
    paths.to_remote(local)
}

/// The remote path built from `local`'s nearest aliased ancestor, if any.
fn grafted_onto_ancestor(paths: &PathMapper, state: &SyncState, local: &Path) -> Option<String> {
    let mut ancestor = local.parent();
    while let Some(dir) = ancestor {
        let relative = paths.relative_key(dir).ok()?;
        if relative.is_empty() {
            return None;
        }
        if let Some(alias) = state.alias_for(&relative) {
            let rest = local.strip_prefix(dir).ok()?;
            let mut remote = alias.to_string();
            for component in rest.components() {
                match component {
                    std::path::Component::Normal(part) => {
                        remote.push('/');
                        remote.push_str(&part.to_string_lossy());
                    }
                    _ => return None,
                }
            }
            return Some(remote);
        }
        ancestor = dir.parent();
    }
    None
}

pub async fn push_path<S: RemoteSink>(
    sink: &S,
    paths: &PathMapper,
    state: &mut SyncState,
    local: &Path,
) -> Result<Pushed> {
    let remote = remote_path_of(paths, state, local)?;
    let Some(metadata) = read_metadata(local).await? else {
        return delete_remote(sink, state, &remote).await;
    };
    if metadata.is_dir() {
        // Directories need no upload of their own: Dropbox creates them
        // implicitly when a file inside one is uploaded.
        return Ok(Pushed::Ignored);
    }
    upload_if_changed(sink, paths, state, &remote, local, &metadata).await
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
    paths: &PathMapper,
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
    let content_hash = hash::hash_file_off_thread(local).await?;
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
    let uploaded = match sink.upload(remote, local, &mode).await {
        Ok(uploaded) => uploaded,
        // The revision we named is stale: the file was edited remotely since we
        // last saw it. Neither version may be dropped, so both are kept.
        Err(Error::Conflict) => {
            return keep_both(sink, paths, state, local, known.as_ref(), metadata).await;
        }
        Err(error) => return Err(error),
    };
    state.insert(
        entry_for_local_file_off_thread(local, &uploaded.path_display, &uploaded.rev).await?,
    );
    Ok(Pushed::Uploaded)
}

/// Keep the local version beside the remote one under a conflicted-copy name.
///
/// The original path is deliberately left alone: the remote version belongs
/// there, and the next pull puts it there. Re-stamping the original's state
/// entry with what is on disk now is what stops the watcher from pushing the
/// same losing write again and conflicting forever.
async fn keep_both<S: RemoteSink>(
    sink: &S,
    paths: &PathMapper,
    state: &mut SyncState,
    local: &Path,
    known: Option<&SyncEntry>,
    metadata: &std::fs::Metadata,
) -> Result<Pushed> {
    let copy = conflict::preserve(local).await?;
    let remote_copy = paths.to_remote(&copy)?;
    let uploaded = sink.upload(&remote_copy, &copy, &WriteMode::Add).await?;
    state.insert(
        entry_for_local_file_off_thread(&copy, &uploaded.path_display, &uploaded.rev).await?,
    );
    if let Some(entry) = known {
        state.insert(refreshed(entry, metadata));
    }
    Ok(Pushed::Conflicted)
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
    use crate::reconcile::testing::Fixture;

    impl Fixture {
        /// Push one local path up through the fixture's own fake account.
        async fn push(&mut self, relative: &str) -> Result<Pushed> {
            let paths = self.paths();
            let local = self.local(relative);
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

    /// A file created inside a locally-shortened folder must upload under the
    /// folder's *real* remote name, grafted from the recorded alias — not
    /// under the fingerprint-mangled on-disk name.
    #[tokio::test]
    async fn a_new_file_in_a_shortened_folder_uploads_to_the_real_remote_path() {
        let mut fixture = Fixture::new();
        // The folder arrived from Dropbox with a name too long for disk, so it
        // lives locally as "long~abc12345" with an alias back to the original.
        fixture
            .state
            .record_alias("long~abc12345", "/A Very Long Folder Name");
        fixture.write("long~abc12345/new.txt", b"fresh");

        assert_eq!(
            fixture.push("long~abc12345/new.txt").await.unwrap(),
            Pushed::Uploaded
        );
        assert_eq!(
            fixture
                .remote
                .content("/A Very Long Folder Name/new.txt")
                .unwrap(),
            b"fresh"
        );
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

    /// The conflict path: Dropbox refuses the write, so our version is kept
    /// beside the remote one instead of either being lost.
    #[tokio::test]
    async fn a_refused_write_becomes_a_conflicted_copy() {
        let mut fixture = Fixture::new();
        fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();
        fixture.remote.refuse_updates("/a.txt", 1);

        fixture.write("a.txt", b"my version");
        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Conflicted);

        // Our bytes went up under the conflicted name...
        assert_eq!(
            fixture.remote.content("/a (conflicted copy).txt").unwrap(),
            b"my version"
        );
        // ...and the original is still on disk for the next pull to replace.
        assert!(fixture.dir.path().join("a.txt").exists());
        assert!(fixture.state.get("/a (conflicted copy).txt").is_some());
    }

    /// Without re-stamping the original, the same losing write would be
    /// retried on every watcher event.
    #[tokio::test]
    async fn a_conflict_is_not_retried_forever() {
        let mut fixture = Fixture::new();
        fixture.write("a.txt", b"hello");
        fixture.push("a.txt").await.unwrap();
        fixture.remote.refuse_updates("/a.txt", 5);
        fixture.write("a.txt", b"my version");
        fixture.push("a.txt").await.unwrap();

        assert_eq!(fixture.push("a.txt").await.unwrap(), Pushed::Unchanged);
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
