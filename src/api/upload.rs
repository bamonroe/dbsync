//! Uploading file content, and deleting remote paths.
//!
//! Dropbox caps a single `files/upload` at 150 MiB and recommends sessions well
//! before that, so anything past [`SESSION_THRESHOLD`] is sent as a chunked
//! upload session instead. Both paths end in the same `FileMetadata`, so the
//! caller does not care which was used.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::client::ApiClient;
use super::metadata::RemoteFile;
use crate::error::{Error, Result};

/// Files at or above this size go through an upload session.
pub const SESSION_THRESHOLD: u64 = 8 * 1024 * 1024;

/// How much is sent per session request. Must be a multiple of 4 MiB per
/// Dropbox's guidance for concurrent sessions, and small enough to hold in
/// memory comfortably.
pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// How Dropbox should treat an existing file at the same path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteMode {
    /// The file is new to us. Dropbox rejects the write if something is there.
    Add,
    /// We believe the remote is still at this revision. If it moved on,
    /// Dropbox refuses rather than clobbering someone else's edit.
    Update(String),
}

impl Serialize for WriteMode {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            // The union's short form: a bare string for the no-argument case.
            Self::Add => serializer.serialize_str("add"),
            Self::Update(rev) => {
                use serde::ser::SerializeStruct;
                let mut tagged = serializer.serialize_struct("update", 2)?;
                tagged.serialize_field(".tag", "update")?;
                tagged.serialize_field("update", rev)?;
                tagged.end()
            }
        }
    }
}

#[derive(Serialize)]
struct UploadArg<'a> {
    path: &'a str,
    mode: &'a WriteMode,
    /// Never let Dropbox invent `file (1).txt` behind our back: a rejected
    /// write is a conflict we want to see and handle ourselves.
    autorename: bool,
    /// This write is dbsync mirroring the user's own file; it should not raise
    /// a desktop notification.
    mute: bool,
}

#[derive(Serialize)]
struct SessionStartArg {
    close: bool,
}

#[derive(Deserialize)]
struct SessionStart {
    session_id: String,
}

#[derive(Serialize)]
struct Cursor<'a> {
    session_id: &'a str,
    offset: u64,
}

#[derive(Serialize)]
struct SessionAppendArg<'a> {
    cursor: Cursor<'a>,
    close: bool,
}

#[derive(Serialize)]
struct SessionFinishArg<'a> {
    cursor: Cursor<'a>,
    commit: UploadArg<'a>,
}

#[derive(Serialize)]
struct DeleteArg<'a> {
    path: &'a str,
}

#[derive(Deserialize)]
struct DeleteResult {
    #[allow(dead_code)]
    metadata: serde_json::Value,
}

impl ApiClient {
    /// Upload `local` to `remote_path`, choosing single-shot or session by size.
    pub async fn upload(
        &self,
        remote_path: &str,
        local: &Path,
        mode: &WriteMode,
    ) -> Result<RemoteFile> {
        let size = tokio::fs::metadata(local).await?.len();
        match size >= SESSION_THRESHOLD {
            true => self.upload_session(remote_path, local, mode).await,
            false => {
                let content = tokio::fs::read(local).await?;
                self.upload_once(remote_path, content, mode).await
            }
        }
    }

    async fn upload_once(
        &self,
        remote_path: &str,
        content: Vec<u8>,
        mode: &WriteMode,
    ) -> Result<RemoteFile> {
        let arg = UploadArg {
            path: remote_path,
            mode,
            autorename: false,
            mute: true,
        };
        let response = self.content_upload("files/upload", &arg, content).await?;
        Ok(response.json().await?)
    }

    /// Send a large file in chunks: start, append until drained, then finish.
    async fn upload_session(
        &self,
        remote_path: &str,
        local: &Path,
        mode: &WriteMode,
    ) -> Result<RemoteFile> {
        let mut file = tokio::fs::File::open(local).await?;
        let start: SessionStart = self
            .content_upload(
                "files/upload_session/start",
                &SessionStartArg { close: false },
                Vec::new(),
            )
            .await?
            .json()
            .await?;

        let mut offset = 0u64;
        loop {
            let chunk = read_chunk(&mut file).await?;
            if chunk.is_empty() {
                break;
            }
            let sent = chunk.len() as u64;
            let arg = SessionAppendArg {
                cursor: Cursor {
                    session_id: &start.session_id,
                    offset,
                },
                close: false,
            };
            self.content_upload("files/upload_session/append_v2", &arg, chunk)
                .await?;
            offset += sent;
        }

        let finish = SessionFinishArg {
            cursor: Cursor {
                session_id: &start.session_id,
                offset,
            },
            commit: UploadArg {
                path: remote_path,
                mode,
                autorename: false,
                mute: true,
            },
        };
        let response = self
            .content_upload("files/upload_session/finish", &finish, Vec::new())
            .await?;
        Ok(response.json().await?)
    }

    /// Delete a remote path. A path that is already gone is not an error.
    pub async fn delete(&self, remote_path: &str) -> Result<()> {
        let arg = DeleteArg { path: remote_path };
        match self.rpc::<_, DeleteResult>("files/delete_v2", &arg).await {
            Ok(_) => Ok(()),
            Err(Error::Api { status: 409, .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Read up to [`CHUNK_SIZE`] bytes; a short read only means end of file.
async fn read_chunk(file: &mut tokio::fs::File) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut chunk = vec![0u8; CHUNK_SIZE];
    let mut filled = 0;
    while filled < CHUNK_SIZE {
        let read = file.read(&mut chunk[filled..]).await?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    chunk.truncate(filled);
    Ok(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arg_json(mode: WriteMode) -> serde_json::Value {
        serde_json::to_value(UploadArg {
            path: "/a.txt",
            mode: &mode,
            autorename: false,
            mute: true,
        })
        .unwrap()
    }

    /// A new file uses the union's short form.
    #[test]
    fn add_serialises_as_a_bare_tag() {
        assert_eq!(arg_json(WriteMode::Add)["mode"], serde_json::json!("add"));
    }

    /// An edit must name the revision we think we are replacing, or a
    /// concurrent remote edit would be silently overwritten.
    #[test]
    fn update_carries_the_revision_it_expects() {
        assert_eq!(
            arg_json(WriteMode::Update("r7".into()))["mode"],
            serde_json::json!({".tag": "update", "update": "r7"})
        );
    }

    /// Autorename would hide conflicts by inventing a new name server-side.
    #[test]
    fn uploads_never_autorename() {
        assert_eq!(
            arg_json(WriteMode::Add)["autorename"],
            serde_json::json!(false)
        );
        assert_eq!(arg_json(WriteMode::Add)["mute"], serde_json::json!(true));
    }

    #[test]
    fn a_session_cursor_names_the_session_and_offset() {
        let json = serde_json::to_value(SessionAppendArg {
            cursor: Cursor {
                session_id: "s1",
                offset: 4096,
            },
            close: false,
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({"cursor": {"session_id": "s1", "offset": 4096}, "close": false})
        );
    }

    /// The chunk size must divide evenly into Dropbox's 4 MiB requirement.
    #[test]
    fn the_chunk_size_is_a_multiple_of_four_mib() {
        assert_eq!(CHUNK_SIZE % (4 * 1024 * 1024), 0);
        assert!(SESSION_THRESHOLD as usize <= CHUNK_SIZE * 2);
    }

    #[tokio::test]
    async fn reading_chunks_covers_a_file_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.bin");
        std::fs::write(&path, vec![7u8; 100]).unwrap();

        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let first = read_chunk(&mut file).await.unwrap();
        let second = read_chunk(&mut file).await.unwrap();
        assert_eq!(first.len(), 100);
        assert!(second.is_empty());
    }
}
