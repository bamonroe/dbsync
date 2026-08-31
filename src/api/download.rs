//! Downloading file content.
//!
//! The body is streamed to a temporary file and renamed into place, so a
//! half-written download can never be mistaken for the real file — the same
//! atomic-replace rule the state database follows.
//!
//! A file takes one of two shapes, decided by its size. A small one arrives on
//! a single stream and is refetched whole if interrupted. A large one is
//! split by [`super::chunks`] into fixed ranges written at their true offsets,
//! where the length means nothing and completion is "every chunk present" —
//! see [`super::partial`]. Only the chunked shape resumes.

use std::path::Path;

use futures_util::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use serde::Serialize;

use super::chunkmap::MAP_SUFFIX;
use super::chunks::{Allowance, ChunkPlan};
use super::client::ApiClient;
use super::partial::Partial;
use super::range::ByteRange;
use crate::error::{Error, Result};
use crate::reconcile::paths::{MAX_COMPONENT_BYTES, shorten_to};

/// Suffix for the partial file a download writes before its rename.
const PARTIAL_SUFFIX: &str = ".dbsync-partial";

/// Tries per chunk before the whole file fails. A chunk is independent, so a
/// transient error costs one range rather than the download.
const CHUNK_ATTEMPTS: u32 = 3;

#[derive(Serialize)]
struct DownloadRequest<'a> {
    path: &'a str,
}

impl ApiClient {
    /// Download revision `rev` of `remote_path` and atomically place it at
    /// `dest`, continuing an earlier interrupted attempt where possible.
    ///
    /// Creates `dest`'s parent directory if it is missing, since a change
    /// stream can deliver a file before the folder that contains it.
    ///
    /// The partial file is keyed by `rev` and a resumed chunk asks for that
    /// same `rev`, which together are what make continuing safe: a range can
    /// only ever be filled with bytes from the revision it came from, so an
    /// edit landing mid-download starts a new partial rather than splicing two
    /// versions of the file together.
    ///
    /// A failed chunked attempt keeps its partial so the next one can resume;
    /// strays from a hard kill are cleared at startup by
    /// [`crate::reconcile::sweep::partial_downloads`].
    ///
    /// `allowance` decides the shape of the fetch: a small file arrives on one
    /// stream and is refetched whole if interrupted, while a large one is split
    /// into fixed chunks written at their true offsets, fetched as concurrently
    /// as the bytes it was admitted for allow, and renamed only once every
    /// chunk is present.
    pub async fn download_to(
        &self,
        remote_path: &str,
        rev: &str,
        allowance: Allowance,
        dest: &Path,
    ) -> Result<()> {
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let plan = ChunkPlan::new(allowance.size, self.chunking());
        if !plan.is_whole_file() {
            return self
                .download_chunked(remote_path, rev, allowance, plan, dest)
                .await;
        }
        self.download_whole(remote_path, rev, dest).await
    }

    /// The single-stream fetch: one request, written out and renamed when the
    /// body ends.
    ///
    /// This path never resumes. A partial's *length* cannot be trusted after a
    /// crash — ext4 can leave the file at full length with an unsynced,
    /// zero-filled tail, and appending after that length would splice garbage
    /// into the middle of the finished file. A file small enough for this path
    /// is cheap to refetch whole; only the chunked path resumes, because it
    /// fsyncs each chunk and gates completion on its chunk map, not on length.
    async fn download_whole(&self, remote_path: &str, rev: &str, dest: &Path) -> Result<()> {
        let partial = partial_path(dest, rev);
        let mut response = self.request_whole(remote_path).await?;
        let mut file = tokio::fs::File::create(&partial).await?;

        // Stream rather than buffer: a synced folder may hold files far larger
        // than the daemon's memory budget.
        stream_to(&mut response, &mut file).await?;
        drop(file);
        tokio::fs::rename(&partial, dest).await?;
        Ok(())
    }

    /// The chunked fetch: every missing range, concurrently, into one
    /// preallocated partial.
    ///
    /// Every chunk addresses `rev:…`, so they provably come from one revision:
    /// a remote edit mid-download cannot splice two versions together, it
    /// simply starts a different partial.
    ///
    /// A chunk that fails is retried on its own and the rest keep their bits;
    /// only a revision-level failure — the range refused outright — throws the
    /// partial away, because in that case no chunk of it can be trusted.
    async fn download_chunked(
        &self,
        remote_path: &str,
        rev: &str,
        allowance: Allowance,
        plan: ChunkPlan,
        dest: &Path,
    ) -> Result<()> {
        let path = partial_path(dest, rev);
        let partial = Partial::open(&path, plan).await?;
        let missing = partial.missing().await;
        let slots = allowance.chunk_slots(plan.chunk_size(), self.chunking());
        tracing::debug!(
            path = remote_path,
            chunks = plan.count(),
            fetching = missing.len(),
            slots,
            "downloading in chunks"
        );

        let outcome = stream_iter(missing)
            .map(|index| self.fetch_chunk(rev, plan, &partial, index))
            .buffer_unordered(slots)
            .try_collect::<Vec<()>>()
            .await;
        match outcome {
            // The revision will not serve the ranges this plan assumes, so the
            // partial describes a file that does not exist. Start clean.
            Err(error @ Error::Api { status: 416, .. }) => {
                tracing::warn!(
                    path = remote_path,
                    "chunked download was unusable; discarding"
                );
                partial.abandon().await;
                Err(error)
            }
            Err(error) => Err(error),
            // Gated on every chunk being present, never on the length: the
            // partial has been full length since the last offset was written.
            Ok(_) => partial.finish(dest).await,
        }
    }

    /// Fetch one chunk into its slot, retrying that range alone.
    ///
    /// A chunk is independent: a retry re-asks for the same bytes of the same
    /// revision, and the chunks that already landed keep their bits.
    async fn fetch_chunk(
        &self,
        rev: &str,
        plan: ChunkPlan,
        partial: &Partial,
        index: u32,
    ) -> Result<()> {
        let Some(range) = plan.range(index) else {
            return Ok(());
        };
        let mut last = None;
        for attempt in 1..=CHUNK_ATTEMPTS {
            match self.try_chunk(rev, range, partial, index).await {
                Ok(()) => return Ok(()),
                // Not worth retrying: the revision does not have these bytes,
                // and asking again will not change that.
                Err(error @ Error::Api { status: 416, .. }) => return Err(error),
                Err(error) => {
                    tracing::debug!(chunk = index, attempt, %error, "retrying chunk");
                    last = Some(error);
                }
            }
        }
        Err(last.expect("a failed chunk records its error"))
    }

    async fn try_chunk(
        &self,
        rev: &str,
        range: ByteRange,
        partial: &Partial,
        index: u32,
    ) -> Result<()> {
        let mut response = self.request_range(rev, range).await?;
        partial.write_chunk(index, &mut response).await
    }

    /// Ask one immutable revision for exactly `range`.
    async fn request_range(&self, rev: &str, range: ByteRange) -> Result<reqwest::Response> {
        self.content_download_from(
            "files/download",
            &DownloadRequest {
                path: &format!("rev:{rev}"),
            },
            range,
        )
        .await
    }

    /// Ask for a whole file from its start.
    async fn request_whole(&self, remote_path: &str) -> Result<reqwest::Response> {
        self.content_download_from(
            "files/download",
            &DownloadRequest { path: remote_path },
            ByteRange::from(0),
        )
        .await
    }
}

async fn stream_to(response: &mut reqwest::Response, file: &mut tokio::fs::File) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
    }
    // Without this the rename could expose an empty file after a crash.
    file.sync_all().await?;
    Ok(())
}

/// The scratch path a download is written to before being renamed onto `dest`.
///
/// Deliberately a sibling of `dest`: a rename is only atomic within one
/// filesystem, so a temp directory elsewhere would not do.
///
/// The revision is part of the name so a partial is only ever resumed into the
/// revision it was fetched from. A file edited remotely mid-download simply
/// gets a different partial, and the stale one is swept later.
///
/// The suffix is added on top of a name that may already sit at the filesystem's
/// component limit, so the base is shortened when it has to be. Only the scratch
/// name pays that cost: `dest` is renamed into place at the end and has the whole
/// budget to itself. The shortened base still identifies the partial uniquely,
/// which is all a resume needs, and the sweep recognises it by its suffix.
fn partial_path(dest: &Path, rev: &str) -> std::path::PathBuf {
    let suffix = format!(".{}{PARTIAL_SUFFIX}", sanitise_rev(rev));
    // The chunk map is named off the partial, so it is the longer of the two and
    // the one that has to fit.
    let room = MAX_COMPONENT_BYTES.saturating_sub(suffix.len() + MAP_SUFFIX.len());
    let base = dest.file_name().unwrap_or_default();
    let mut name = match base.to_str() {
        Some(base) => std::ffi::OsString::from(shorten_to(base, room)),
        // Not valid UTF-8, so it cannot be fingerprinted; leave it alone rather
        // than risk splitting a byte sequence.
        None => base.to_os_string(),
    };
    name.push(suffix);
    dest.with_file_name(name)
}

/// Where a finished download parks before the applier's final rename.
///
/// The applier re-checks for a local edit *after* the transfer and only then
/// renames this over the real path, so a slow download cannot silently clobber
/// an edit made while it ran. The name carries the partial suffix so the
/// watcher ignores it and the startup sweep clears strays, and it is shortened
/// the same way a partial is so a maximal name still fits.
pub fn staged_path(dest: &Path) -> std::path::PathBuf {
    let suffix = format!(".staged{PARTIAL_SUFFIX}");
    // The download's own partial and chunk map are named off this path, so
    // leave them room too.
    let room = MAX_COMPONENT_BYTES.saturating_sub(2 * suffix.len() + 32 + MAP_SUFFIX.len());
    let base = dest.file_name().unwrap_or_default();
    let mut name = match base.to_str() {
        Some(base) => std::ffi::OsString::from(shorten_to(base, room)),
        None => base.to_os_string(),
    };
    name.push(suffix);
    dest.with_file_name(name)
}

/// Keep a revision usable as part of a filename.
///
/// Dropbox revisions are hex, but nothing in the API promises that, and a `/`
/// arriving here would silently redirect the write somewhere else.
fn sanitise_rev(rev: &str) -> String {
    rev.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(32)
        .collect()
}

/// Is this a leftover partial download rather than a real synced file?
pub fn is_partial(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            name.ends_with(PARTIAL_SUFFIX)
                || name.ends_with(&format!("{PARTIAL_SUFFIX}{MAP_SUFFIX}"))
        })
}

/// Does this partial carry resumable progress worth keeping across a restart?
///
/// Only a chunked download resumes, and its progress lives in the sidecar map:
/// a partial with its map beside it is a resume in waiting, and the map is
/// kept for as long as its partial is. Everything else — a single-stream
/// scratch file, a staged download, an orphaned map — is dead weight.
pub fn is_resumable_partial(path: &Path) -> bool {
    if !is_partial(path) {
        return false;
    }
    let Some(name) = path.to_str() else {
        return false;
    };
    match name.strip_suffix(MAP_SUFFIX) {
        Some(partial) => Path::new(partial).exists(),
        None => super::chunkmap::sidecar_path(path).exists(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn the_partial_sits_beside_its_destination() {
        let dest = PathBuf::from("/home/me/Dropbox/notes/a.txt");
        let partial = partial_path(&dest, "0159abc");
        assert_eq!(partial.parent(), dest.parent());
        assert_ne!(partial, dest);
    }

    #[test]
    fn a_partial_is_recognisable_and_a_real_file_is_not() {
        assert!(is_partial(&partial_path(Path::new("/tmp/a.txt"), "r1")));
        assert!(!is_partial(Path::new("/tmp/a.txt")));
    }

    /// Two revisions must not share a partial, or a resume could append the
    /// bytes of one revision onto the prefix of another.
    #[test]
    fn each_revision_gets_its_own_partial() {
        let dest = Path::new("/tmp/a.txt");
        assert_ne!(partial_path(dest, "r1"), partial_path(dest, "r2"));
    }

    /// A revision is used to build a path, so a separator in one would write
    /// somewhere else entirely.
    #[test]
    fn a_revision_cannot_escape_its_directory() {
        let partial = partial_path(Path::new("/tmp/a.txt"), "../../etc/x");
        assert_eq!(partial.parent(), Path::new("/tmp").into());
        assert!(is_partial(&partial));
    }

    #[test]
    fn a_long_revision_does_not_run_away_with_the_filename() {
        let partial = partial_path(Path::new("/tmp/a.txt"), &"a".repeat(500));
        let name = partial.file_name().unwrap().to_str().unwrap();
        assert!(name.len() < 100, "filename was {} bytes", name.len());
    }

    /// The real name is allowed to sit at the limit, so the partial's suffix —
    /// and the chunk map's on top of it — has to come out of the base.
    #[test]
    fn a_partial_of_a_maximal_name_still_fits() {
        let dest = Path::new("/tmp").join("a".repeat(MAX_COMPONENT_BYTES));
        let partial = partial_path(&dest, "0159abc");
        let name = partial.file_name().unwrap().to_str().unwrap();
        assert!(
            name.len() <= MAX_COMPONENT_BYTES,
            "partial was {}",
            name.len()
        );
        assert!(
            name.len() + MAP_SUFFIX.len() <= MAX_COMPONENT_BYTES,
            "chunk map would not fit"
        );
        assert!(is_partial(&partial));
    }

    /// Shortening must not merge two long names onto one scratch file, or a
    /// resume would append one file's bytes onto another's prefix.
    #[test]
    fn two_long_names_sharing_a_prefix_get_different_partials() {
        let shared = "b".repeat(MAX_COMPONENT_BYTES);
        let one = Path::new("/tmp").join(format!("{shared}one"));
        let two = Path::new("/tmp").join(format!("{shared}two"));
        assert_ne!(partial_path(&one, "r1"), partial_path(&two, "r1"));
    }

    /// A short name is left exactly as it was.
    #[test]
    fn an_ordinary_name_is_not_shortened() {
        let partial = partial_path(Path::new("/tmp/a.txt"), "r1");
        assert_eq!(
            partial.file_name().unwrap().to_str().unwrap(),
            "a.txt.r1.dbsync-partial"
        );
    }

    #[test]
    fn download_asks_for_one_path() {
        let json = serde_json::to_value(DownloadRequest { path: "/a/b.txt" }).unwrap();
        assert_eq!(json, serde_json::json!({"path": "/a/b.txt"}));
    }
}
