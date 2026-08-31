//! Conflicted copies: what happens when both sides edited the same file.
//!
//! dbsync never resolves a conflict by choosing a winner. When the local and
//! remote versions have diverged, the local bytes are set aside as
//! `name (conflicted copy).ext` next to the original and the remote version
//! takes the original path — the same shape the official desktop client uses,
//! and the reason `docs/architecture.md` calls losing data the one unacceptable
//! outcome.
//!
//! The copy is a **copy**, not a rename. Renaming would make the original path
//! disappear, and the watcher would read that as a deletion and delete the file
//! from Dropbox — destroying exactly the version we were trying to protect.

use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::state::{SyncState, hash};

/// Inserted before the extension, so the copy sorts next to the original.
const MARKER: &str = "conflicted copy";

/// Set the current local bytes aside, and return where they went.
///
/// The target is *claimed* with `O_EXCL` rather than probed with `exists()`:
/// a probe-then-copy would let two near-simultaneous conflicts on one file —
/// or an incoming download that already carries a conflicted-copy name —
/// truncate an earlier copy, which is exactly the bytes this exists to keep.
pub async fn preserve(local: &Path) -> Result<PathBuf> {
    use tokio::io::AsyncWriteExt;
    let (target, mut file) = claim_beside(local).await?;
    let mut source = tokio::fs::File::open(local).await?;
    tokio::io::copy(&mut source, &mut file).await?;
    file.flush().await?;
    let permissions = source.metadata().await?.permissions();
    tokio::fs::set_permissions(&target, permissions).await?;
    tracing::warn!(
        original = %local.display(),
        copy = %target.display(),
        "diverging edits; kept the local version as a conflicted copy"
    );
    Ok(target)
}

/// Exclusively create the first free conflicted-copy name beside `local`.
async fn claim_beside(local: &Path) -> Result<(PathBuf, tokio::fs::File)> {
    for nth in 1.. {
        let candidate = numbered(local, nth);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(file) => return Ok((candidate, file)),
            // Someone else holds this name; take the next one.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!("the loop runs until a name is free")
}

/// The first free `name (conflicted copy).ext` beside `local`.
///
/// Conflicts repeat — a file edited on two machines all afternoon produces
/// several — so the name is numbered once the plain one is taken. This only
/// *names* the first free slot; [`preserve`] claims one atomically.
pub fn conflicted_path(local: &Path) -> PathBuf {
    let mut nth = 1;
    let mut candidate = numbered(local, nth);
    while candidate.exists() {
        nth += 1;
        candidate = numbered(local, nth);
    }
    candidate
}

/// The `nth` conflicted-copy name for `local`; the first is unnumbered.
fn numbered(local: &Path, nth: u32) -> PathBuf {
    let stem = local
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let extension = local
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let parent = local.parent().unwrap_or(Path::new(""));
    match nth {
        1 => parent.join(format!("{stem} ({MARKER}){extension}")),
        _ => parent.join(format!("{stem} ({MARKER} {nth}){extension}")),
    }
}

/// Does the file on disk hold bytes we have never sent to Dropbox?
///
/// This is the question that decides whether an incoming download is a plain
/// update or a conflict. Three cases say no: the file is not there, the state's
/// cheap metadata check still matches, or the content hash does. Anything else
/// — including a file we have never tracked at all — is a local edit that must
/// not be overwritten.
/// `incoming_hash` is the content hash of the remote version about to land.
/// Dropbox omits it on some entries, in which case an untracked local file has
/// to be treated as a conflict — losing a copy is worse than keeping a spare.
pub async fn has_local_edit(
    state: &SyncState,
    display_path: &str,
    local: &Path,
    incoming_hash: Option<&str>,
) -> Result<bool> {
    let Ok(metadata) = tokio::fs::metadata(local).await else {
        return Ok(false);
    };
    if metadata.is_dir() {
        return Ok(false);
    }
    let Some(entry) = state.get(display_path) else {
        // Untracked but present. Usually someone put a file where the remote
        // has one — but it is also what our own interrupted download looks
        // like, because the file lands in place before the state that records
        // it is checkpointed. Identical content is not a divergence either
        // way, so compare before crying conflict; otherwise every restart
        // mid-pull mints a spurious conflicted copy of its own work.
        return match incoming_hash {
            Some(remote) => Ok(hash::hash_file_off_thread(local).await? != remote),
            None => Ok(true),
        };
    };
    if entry.metadata_matches(metadata.len(), metadata.modified()?) {
        return Ok(false);
    }
    Ok(entry.content_hash != hash::hash_file_off_thread(local).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_marker_goes_before_the_extension() {
        assert_eq!(
            conflicted_path(Path::new("/tmp/dbsync-none/notes.txt")),
            PathBuf::from("/tmp/dbsync-none/notes (conflicted copy).txt")
        );
    }

    #[test]
    fn a_file_without_an_extension_still_gets_a_name() {
        assert_eq!(
            conflicted_path(Path::new("/tmp/dbsync-none/README")),
            PathBuf::from("/tmp/dbsync-none/README (conflicted copy)")
        );
    }

    /// A dotfile's leading dot is its name, not an extension.
    #[test]
    fn a_dotfile_keeps_its_leading_dot() {
        assert_eq!(
            conflicted_path(Path::new("/tmp/dbsync-none/.bashrc")),
            PathBuf::from("/tmp/dbsync-none/.bashrc (conflicted copy)")
        );
    }

    /// Repeated conflicts must not overwrite each other.
    #[test]
    fn a_taken_name_is_numbered() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("a.txt");
        std::fs::write(&original, b"x").unwrap();
        std::fs::write(dir.path().join("a (conflicted copy).txt"), b"x").unwrap();

        assert_eq!(
            conflicted_path(&original),
            dir.path().join("a (conflicted copy 2).txt")
        );
    }

    /// A copy already sitting at the name must never be truncated — preserve
    /// claims the *next* free slot instead.
    #[tokio::test]
    async fn preserving_never_overwrites_an_existing_copy() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("a.txt");
        std::fs::write(&original, b"newest").unwrap();
        let taken = dir.path().join("a (conflicted copy).txt");
        std::fs::write(&taken, b"earlier bytes").unwrap();

        let copy = preserve(&original).await.unwrap();

        assert_eq!(copy, dir.path().join("a (conflicted copy 2).txt"));
        assert_eq!(std::fs::read(&taken).unwrap(), b"earlier bytes");
        assert_eq!(std::fs::read(&copy).unwrap(), b"newest");
    }

    #[tokio::test]
    async fn preserving_leaves_the_original_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("a.txt");
        std::fs::write(&original, b"mine").unwrap();

        let copy = preserve(&original).await.unwrap();

        assert_eq!(std::fs::read(&copy).unwrap(), b"mine");
        assert!(original.exists(), "the original must survive the copy");
    }

    #[tokio::test]
    async fn a_missing_file_is_not_an_edit() {
        let state = SyncState::new();
        let missing = Path::new("/tmp/dbsync-none/gone.txt");
        assert!(
            !has_local_edit(&state, "/gone.txt", missing, None)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn an_untracked_file_counts_as_an_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"mine").unwrap();
        assert!(
            has_local_edit(&SyncState::new(), "/a.txt", &path, None)
                .await
                .unwrap()
        );
    }

    /// The interrupted-download case: we wrote this file ourselves and died
    /// before recording it. Identical bytes are not a divergence, and calling
    /// them one is what filled the tree with spurious conflicted copies.
    #[tokio::test]
    async fn an_untracked_file_matching_the_incoming_content_is_not_an_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"same").unwrap();
        let incoming = crate::state::hash::hash_bytes(b"same");

        assert!(
            !has_local_edit(&SyncState::new(), "/a.txt", &path, Some(&incoming))
                .await
                .unwrap()
        );
    }

    /// A genuinely different untracked file is still a conflict.
    #[tokio::test]
    async fn an_untracked_file_differing_from_the_incoming_content_is_an_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"mine").unwrap();
        let incoming = crate::state::hash::hash_bytes(b"theirs");

        assert!(
            has_local_edit(&SyncState::new(), "/a.txt", &path, Some(&incoming))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_file_matching_the_state_is_not_an_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"mine").unwrap();
        let mut state = SyncState::new();
        state.insert(crate::state::entry_for_local_file(&path, "/a.txt", "r1").unwrap());

        assert!(!has_local_edit(&state, "/a.txt", &path, None).await.unwrap());
    }
}
