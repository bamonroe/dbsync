//! Downloading file content.
//!
//! The body is streamed to a temporary file and renamed into place, so a
//! half-written download can never be mistaken for the real file — the same
//! atomic-replace rule the state database follows.

use std::path::Path;

use serde::Serialize;

use super::client::ApiClient;
use super::range::ByteRange;
use crate::error::{Error, Result};

/// Suffix for the partial file a download writes before its rename.
const PARTIAL_SUFFIX: &str = ".dbsync-partial";

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
    /// The partial file is keyed by `rev` and the resumed request asks for that
    /// same `rev`, which together are what make continuing safe: a prefix can
    /// only ever be extended with bytes from the revision it came from, so an
    /// edit landing mid-download starts a new partial rather than splicing two
    /// versions of the file together.
    ///
    /// A failed attempt now *keeps* its partial so the next one can resume;
    /// strays from a hard kill are cleared at startup by
    /// [`crate::reconcile::sweep::partial_downloads`].
    ///
    /// `size` is the revision's length. Nothing needs it while the whole file
    /// arrives on one stream, but chunked fetching cannot plan its ranges
    /// without it, and the caller has had it all along.
    pub async fn download_to(
        &self,
        remote_path: &str,
        rev: &str,
        _size: u64,
        dest: &Path,
    ) -> Result<()> {
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let partial = partial_path(dest, rev);
        let have = existing_len(&partial).await;

        let mut response = match self.request_from(remote_path, rev, have).await {
            // The range is past the end of the file: the partial is longer than
            // the revision it claims to be, so it is garbage. Start over.
            Err(Error::Api { status: 416, .. }) => {
                tracing::warn!(
                    path = remote_path,
                    "partial download was unusable; restarting"
                );
                let _ = tokio::fs::remove_file(&partial).await;
                self.request_from(remote_path, rev, 0).await?
            }
            other => other?,
        };

        // A server that ignored the range answers 200 with the whole file, so
        // the partial has to be truncated rather than appended to.
        let resuming = have > 0 && response.status() == reqwest::StatusCode::PARTIAL_CONTENT;
        let mut file = open_partial(&partial, resuming).await?;
        if resuming {
            tracing::debug!(path = remote_path, resumed_at = have, "resuming download");
        }

        // Stream rather than buffer: a synced folder may hold files far larger
        // than the daemon's memory budget.
        stream_to(&mut response, &mut file).await?;
        drop(file);
        tokio::fs::rename(&partial, dest).await?;
        Ok(())
    }

    /// Ask for `rev`, from `offset` onwards when that is not the start.
    async fn request_from(
        &self,
        remote_path: &str,
        rev: &str,
        offset: u64,
    ) -> Result<reqwest::Response> {
        // `rev:…` addresses one immutable revision; the display path would
        // return whatever is current, which is not what a resume may append to.
        let by_rev = format!("rev:{rev}");
        let path = match offset {
            0 => remote_path,
            _ => &by_rev,
        };
        self.content_download_from(
            "files/download",
            &DownloadRequest { path },
            ByteRange::from(offset),
        )
        .await
    }
}

/// How many bytes of this download are already on disk.
async fn existing_len(partial: &Path) -> u64 {
    tokio::fs::metadata(partial)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0)
}

/// Open the partial for appending when resuming, or truncate it when not.
async fn open_partial(partial: &Path, resuming: bool) -> Result<tokio::fs::File> {
    if !resuming {
        return Ok(tokio::fs::File::create(partial).await?);
    }
    Ok(tokio::fs::OpenOptions::new()
        .append(true)
        .open(partial)
        .await?)
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
fn partial_path(dest: &Path, rev: &str) -> std::path::PathBuf {
    let mut name = dest.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}{PARTIAL_SUFFIX}", sanitise_rev(rev)));
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
        .is_some_and(|name| name.ends_with(PARTIAL_SUFFIX))
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

    #[test]
    fn download_asks_for_one_path() {
        let json = serde_json::to_value(DownloadRequest { path: "/a/b.txt" }).unwrap();
        assert_eq!(json, serde_json::json!({"path": "/a/b.txt"}));
    }
}
