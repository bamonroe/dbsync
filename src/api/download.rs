//! Downloading file content.
//!
//! The body is streamed to a temporary file and renamed into place, so a
//! half-written download can never be mistaken for the real file — the same
//! atomic-replace rule the state database follows.

use std::path::Path;

use serde::Serialize;

use super::client::ApiClient;
use crate::error::Result;

/// Suffix for the partial file a download writes before its rename.
const PARTIAL_SUFFIX: &str = ".dbsync-partial";

#[derive(Serialize)]
struct DownloadRequest<'a> {
    path: &'a str,
}

impl ApiClient {
    /// Download `remote_path` and atomically place it at `dest`.
    ///
    /// Creates `dest`'s parent directory if it is missing, since a change
    /// stream can deliver a file before the folder that contains it.
    pub async fn download_to(&self, remote_path: &str, dest: &Path) -> Result<()> {
        let mut response = self
            .content_download("files/download", &DownloadRequest { path: remote_path })
            .await?;

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let partial = partial_path(dest);
        let mut file = tokio::fs::File::create(&partial).await?;

        // Stream rather than buffer: a synced folder may hold files far larger
        // than the daemon's memory budget.
        let result = stream_to(&mut response, &mut file).await;
        drop(file);
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(error);
        }
        tokio::fs::rename(&partial, dest).await?;
        Ok(())
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
fn partial_path(dest: &Path) -> std::path::PathBuf {
    let mut name = dest.file_name().unwrap_or_default().to_os_string();
    name.push(PARTIAL_SUFFIX);
    dest.with_file_name(name)
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
        let partial = partial_path(&dest);
        assert_eq!(partial.parent(), dest.parent());
        assert_ne!(partial, dest);
    }

    #[test]
    fn a_partial_is_recognisable_and_a_real_file_is_not() {
        assert!(is_partial(&partial_path(Path::new("/tmp/a.txt"))));
        assert!(!is_partial(Path::new("/tmp/a.txt")));
    }

    #[test]
    fn download_asks_for_one_path() {
        let json = serde_json::to_value(DownloadRequest { path: "/a/b.txt" }).unwrap();
        assert_eq!(json, serde_json::json!({"path": "/a/b.txt"}));
    }
}
