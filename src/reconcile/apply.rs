//! Applying one remote entry to the local disk.
//!
//! Applying happens in three phases, and the split is load-bearing rather than
//! cosmetic. [`decide`] reads the state, [`fetch`] does the network and disk
//! work and reads nothing, and [`record`] writes the state. The exclusive
//! borrow of [`SyncState`] is therefore held either side of the download but
//! never *across* it — which is what allows more than one file to be in flight
//! at once, since concurrent downloads cannot each hold `&mut` on one state.
//!
//! Run in order the three phases still leave the state describing exactly what
//! the disk holds, so the state never claims a file that is not there.

use std::path::{Path, PathBuf};

use crate::api::{Allowance, RemoteEntry, RemoteFile};
use crate::error::{Error, Result};
use crate::state::{SyncState, entry_for_local_file, key_for};

use super::conflict;
use super::dircase;
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

/// What applying one entry will do, decided before any work happens.
///
/// Borrowing from the entry rather than copying out of it keeps the decision
/// free: a plan is only ever used while the page that produced it is alive.
pub(crate) struct Plan<'a> {
    /// Where on disk this entry lands.
    local: PathBuf,
    action: Action<'a>,
}

impl Plan<'_> {
    /// How many bytes this plan will pull, for the admission budget. Anything
    /// that is not a download costs nothing.
    pub(crate) fn size(&self) -> u64 {
        match &self.action {
            Action::Download { file, .. } => file.size,
            _ => 0,
        }
    }
}

/// The work a [`Plan`] calls for.
enum Action<'a> {
    /// Already correct locally — a remote echo of what we hold.
    Skip,
    /// Fetch this revision, first setting the local bytes aside when
    /// `preserve` says they diverged from what we last recorded.
    Download {
        file: &'a RemoteFile,
        preserve: bool,
    },
    /// Create a directory, and remember the casing it carries — this entry is
    /// the only place Dropbox states it reliably. See [`super::dircase`].
    Directory { canonical: String },
    /// Remove this path and forget everything under it.
    Delete { display_path: &'a str },
}

/// Apply one entry from a listing or change stream.
pub async fn apply_entry<S: RemoteSource>(
    source: &S,
    paths: &PathMapper,
    state: &mut SyncState,
    entry: &RemoteEntry,
) -> Result<Applied> {
    let plan = decide(paths, state, entry)?;
    // No admission control on this path: a one-off apply spends the whole file.
    let applied = fetch(source, &plan, plan.size()).await?;
    record(state, &plan)?;
    Ok(applied)
}

/// Phase one: work out what to do, reading the state but touching nothing.
pub(crate) fn decide<'a>(
    paths: &PathMapper,
    state: &SyncState,
    entry: &'a RemoteEntry,
) -> Result<Plan<'a>> {
    // Dropbox only capitalises the last component, so the folders above it are
    // rebuilt from their own entries before this becomes a path on disk.
    let display = dircase::canonical(state, entry.display_path());
    let local = paths.to_local(&display)?;
    let action = match entry {
        RemoteEntry::File(file) => decide_file(state, file, &local)?,
        RemoteEntry::Folder(_) => Action::Directory {
            canonical: dircase::relative(&display).to_string(),
        },
        RemoteEntry::Deleted(_) => Action::Delete {
            display_path: entry.display_path(),
        },
    };
    Ok(Plan { local, action })
}

/// Download a file unless we already hold exactly that revision.
fn decide_file<'a>(state: &SyncState, file: &'a RemoteFile, local: &Path) -> Result<Action<'a>> {
    if is_current(state, file, local) {
        return Ok(Action::Skip);
    }
    // Both sides moved: the remote version is about to land on this path, so
    // the local bytes have to be set aside first or they are gone for good.
    // This check reads the state, so it belongs here and not beside the
    // download — asking later would reopen the window that spurious conflicted
    // copies came through.
    let preserve = conflict::has_local_edit(
        state,
        &file.path_display,
        local,
        file.content_hash.as_deref(),
    )?;
    Ok(Action::Download { file, preserve })
}

/// Phase two: do the network and disk work. Reads no state and writes none,
/// which is what makes this the phase that can run many at a time.
///
/// `budgeted` is how many of the file's bytes admission control reserved. It
/// travels down to the fetch because a chunked download spends that same
/// reservation on its own chunks rather than opening a second pool of sockets.
pub(crate) async fn fetch<S: RemoteSource>(
    source: &S,
    plan: &Plan<'_>,
    budgeted: u64,
) -> Result<Applied> {
    match &plan.action {
        Action::Skip => Ok(Applied::AlreadyCurrent),
        Action::Download { file, preserve } => {
            if *preserve {
                conflict::preserve(&plan.local).await?;
            }
            source
                .download_to(
                    &file.path_display,
                    &file.rev,
                    Allowance {
                        size: file.size,
                        budgeted,
                    },
                    &plan.local,
                )
                .await?;
            Ok(match *preserve {
                true => Applied::Conflicted,
                false => Applied::Downloaded,
            })
        }
        Action::Directory { .. } => {
            if let Some(parent) = plan.local.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            recase_existing(&plan.local).await?;
            tokio::fs::create_dir_all(&plan.local).await?;
            Ok(Applied::Directory)
        }
        Action::Delete { .. } => remove_local(&plan.local).await,
    }
}

/// Phase three: bring the state up to date with what [`fetch`] just did.
///
/// Only reached once the fetch succeeded, so a failed download or a directory
/// that could not be removed leaves the state alone and the path is re-applied
/// when the change stream next mentions it.
pub(crate) fn record(state: &mut SyncState, plan: &Plan<'_>) -> Result<()> {
    match &plan.action {
        Action::Download { file, .. } => {
            // Re-describe from disk rather than from the metadata: the hash and
            // mtime must be the ones a local scan will actually see, or the very
            // next scan would read this file as a local edit and upload it
            // straight back.
            let entry = entry_for_local_file(&plan.local, &file.path_display, &file.rev)?;
            state.insert(entry);
        }
        Action::Delete { display_path } => forget_subtree(state, display_path),
        Action::Directory { canonical } => state.record_folder_case(canonical),
        Action::Skip => {}
    }
    Ok(())
}

/// Rename a sibling that differs from `wanted` only in case.
///
/// Folders created before their casing was known are still on disk under the
/// wrong name, and on a case-sensitive filesystem making the right one simply
/// leaves two. Renaming moves the contents across in one step instead.
///
/// Does nothing when the correct name already exists — that means both are
/// present, and merging them is not a rename but a decision about colliding
/// files, which belongs to whoever made them.
async fn recase_existing(wanted: &Path) -> Result<()> {
    if tokio::fs::symlink_metadata(wanted).await.is_ok() {
        return Ok(());
    }
    let (Some(parent), Some(name)) = (wanted.parent(), wanted.file_name()) else {
        return Ok(());
    };
    let name = name.to_string_lossy().to_lowercase();
    let mut listing = tokio::fs::read_dir(parent).await?;
    while let Some(sibling) = listing.next_entry().await? {
        if !sibling.file_type().await?.is_dir() {
            continue;
        }
        if sibling.file_name().to_string_lossy().to_lowercase() != name {
            continue;
        }
        tracing::info!(
            from = %sibling.path().display(),
            to = %wanted.display(),
            "correcting a folder's capitalisation"
        );
        tokio::fs::rename(sibling.path(), wanted).await?;
        return Ok(());
    }
    Ok(())
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
///
/// The state half of a delete is [`record`]'s job; this only touches the disk.
async fn remove_local(local: &Path) -> Result<Applied> {
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

    /// The listing's size has to reach the fetch: a chunked download plans its
    /// byte ranges from it, and a zero would silently collapse to one request.
    #[tokio::test]
    async fn the_listed_size_reaches_the_download() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"hello");
        let RemoteEntry::File(mut listed) = file("/a.txt", "r1") else {
            unreachable!()
        };
        listed.size = 5;

        fixture.apply(&RemoteEntry::File(listed)).await.unwrap();
        assert_eq!(
            fixture.remote.sizes_asked(),
            vec![("/a.txt".to_string(), 5)]
        );
    }

    /// A folder created before its casing was known must be renamed, not
    /// left beside a second directory holding nothing.
    #[tokio::test]
    async fn a_wrongly_cased_folder_is_renamed_rather_than_duplicated() {
        let dir = tempfile::tempdir().unwrap();
        let wrong = dir.path().join("jri paper");
        tokio::fs::create_dir_all(&wrong).await.unwrap();
        tokio::fs::write(wrong.join("a.txt"), b"hi").await.unwrap();

        let right = dir.path().join("JRI paper");
        recase_existing(&right).await.unwrap();

        assert!(!wrong.exists(), "the wrongly-cased folder was left behind");
        assert_eq!(tokio::fs::read(right.join("a.txt")).await.unwrap(), b"hi");
    }

    /// Both names present is a collision, not a rename: merging them would be
    /// deciding which of two real files wins.
    #[tokio::test]
    async fn a_folder_already_correctly_cased_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let wrong = dir.path().join("notes");
        let right = dir.path().join("Notes");
        tokio::fs::create_dir_all(&wrong).await.unwrap();
        tokio::fs::create_dir_all(&right).await.unwrap();

        recase_existing(&right).await.unwrap();

        assert!(wrong.exists() && right.exists());
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

    /// The decision phase needs no remote and touches nothing, which is the
    /// property the concurrent download path is built on. These cover it
    /// directly rather than through `apply_entry`.
    mod decide {
        use super::*;

        /// Decide against a fresh state and an empty directory.
        fn plan_for<'a>(fixture: &Fixture, entry: &'a RemoteEntry) -> Plan<'a> {
            decide(&fixture.paths(), &fixture.state, entry).unwrap()
        }

        #[test]
        fn an_unknown_file_is_a_download_with_nothing_to_preserve() {
            let fixture = Fixture::new();
            let entry = file("/a.txt", "r1");

            let plan = plan_for(&fixture, &entry);

            assert!(matches!(
                plan.action,
                Action::Download {
                    preserve: false,
                    ..
                }
            ));
            assert_eq!(plan.local, fixture.local("a.txt"));
        }

        /// An untracked file already sitting on the path is someone else's
        /// work until proven otherwise, so it is preserved before the download.
        #[test]
        fn an_untracked_local_file_is_a_download_that_preserves() {
            let fixture = Fixture::new();
            std::fs::write(fixture.local("a.txt"), b"mine").unwrap();
            let entry = file("/a.txt", "r1");

            let plan = plan_for(&fixture, &entry);

            assert!(matches!(
                plan.action,
                Action::Download { preserve: true, .. }
            ));
        }

        /// The revision we already hold is a no-op, and deciding that must not
        /// need the network — this is what keeps a re-list cheap.
        #[test]
        fn a_revision_we_already_hold_is_skipped() {
            let mut fixture = Fixture::new();
            std::fs::write(fixture.local("a.txt"), b"hello").unwrap();
            fixture.state.insert(SyncEntry {
                display_path: "/a.txt".into(),
                rev: "r1".into(),
                ..entry_for_local_file(&fixture.local("a.txt"), "/a.txt", "r1").unwrap()
            });
            let entry = file("/a.txt", "r1");

            assert!(matches!(plan_for(&fixture, &entry).action, Action::Skip));
        }

        #[test]
        fn a_tombstone_is_a_delete_and_a_folder_is_a_directory() {
            let fixture = Fixture::new();
            let tombstone = deleted("/gone.txt");
            let dir = folder("/photos");

            assert!(matches!(
                plan_for(&fixture, &tombstone).action,
                Action::Delete {
                    display_path: "/gone.txt"
                }
            ));
            assert!(matches!(
                plan_for(&fixture, &dir).action,
                Action::Directory { .. }
            ));
        }

        /// A path that escapes the sync root is rejected in the decision, so no
        /// download is ever started for it.
        #[test]
        fn an_escaping_path_fails_before_any_work() {
            let fixture = Fixture::new();
            let entry = file("/../escape.txt", "r1");

            assert!(decide(&fixture.paths(), &fixture.state, &entry).is_err());
        }
    }

    /// Recording a delete only happens once the disk removal succeeded, so a
    /// removal that fails leaves the state describing what is still there.
    /// Otherwise the files would survive untracked and be uploaded back.
    #[tokio::test]
    async fn a_failed_delete_leaves_the_state_alone() {
        let mut fixture = Fixture::new();
        fixture.remote.put("/a.txt", b"a");
        fixture.apply(&file("/a.txt", "r1")).await.unwrap();
        // A directory where the tombstone expects a file: made read-only so the
        // removal underneath it cannot succeed.
        let doomed = fixture.local("locked");
        std::fs::create_dir(&doomed).unwrap();
        std::fs::write(doomed.join("child.txt"), b"child").unwrap();
        let mut permissions = std::fs::metadata(&doomed).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&doomed, permissions).unwrap();
        fixture.state.insert(SyncEntry {
            display_path: "/locked/child.txt".into(),
            ..entry_for_local_file(&doomed.join("child.txt"), "/locked/child.txt", "r1").unwrap()
        });

        let result = fixture.apply(&deleted("/locked")).await;

        // Restore permissions first so the temp directory can be cleaned up.
        let mut permissions = std::fs::metadata(&doomed).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        std::fs::set_permissions(&doomed, permissions).unwrap();

        assert!(result.is_err());
        assert!(fixture.state.get("/locked/child.txt").is_some());
    }
}
