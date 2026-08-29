//! The partial file a chunked download writes into, and the one condition
//! under which it may become the real file.
//!
//! Chunks are written at their **true offsets** into a preallocated file, so
//! the bytes are correct by construction: chunk 5 lands where chunk 5 belongs
//! whether or not chunk 4 has arrived. What that costs is the old completion
//! test. A sparse file reaches its full length the moment the *last* chunk
//! lands, so "length equals size" would rename a file still full of holes into
//! place — silent corruption, the worst failure a sync client has.
//!
//! So the rename is gated on the [`ChunkMap`](super::chunkmap::ChunkMap)
//! instead: **every chunk present**, or nothing moves. The sidecar is removed
//! only *after* the rename succeeds, so a crash in between leaves a complete
//! partial that the next attempt adopts rather than refetches.

// The chunked path is wired up by the concurrent-fetch change; until then this
// is exercised by its own tests.
#![cfg_attr(not(test), allow(dead_code))]

use std::path::{Path, PathBuf};

use super::chunkmap::ChunkMap;
use super::chunks::ChunkPlan;
use crate::error::{Error, Result};

/// A partial download in progress: the file, its plan, and what has landed.
pub(super) struct Partial {
    path: PathBuf,
    plan: ChunkPlan,
    map: ChunkMap,
    file: tokio::fs::File,
}

impl Partial {
    /// Open the partial at `path` for `plan`, adopting whatever an earlier
    /// attempt left behind.
    ///
    /// The file is preallocated to the full size so every chunk has somewhere
    /// to land. On a filesystem with sparse files that costs no space until
    /// the bytes actually arrive.
    pub(super) async fn open(path: &Path, plan: ChunkPlan) -> Result<Self> {
        let map = ChunkMap::open(path, plan).await;
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .await?;
        file.set_len(plan.size()).await?;
        Ok(Self {
            path: path.to_path_buf(),
            plan,
            map,
            file,
        })
    }

    /// The chunks still to fetch. Empty means the download is done.
    pub(super) fn missing(&self) -> Vec<u32> {
        self.map.missing().collect()
    }

    /// Stream one chunk's body into its slot and record it.
    ///
    /// The body must be exactly as long as the chunk: a short one would leave
    /// a hole inside a chunk the map then calls present, which no later check
    /// could detect.
    pub(super) async fn write_chunk(
        &mut self,
        index: u32,
        response: &mut reqwest::Response,
    ) -> Result<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        let Some(expected) = self.chunk_len(index) else {
            return Ok(());
        };
        let offset = u64::from(index) * self.plan.chunk_size();
        self.file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut written = 0_u64;
        while let Some(bytes) = response.chunk().await? {
            written += bytes.len() as u64;
            if written > expected {
                return Err(self.overrun(index, written, expected));
            }
            self.file.write_all(&bytes).await?;
        }
        if written != expected {
            return Err(self.overrun(index, written, expected));
        }
        // The bytes must be on disk before the bit that claims them is, or a
        // crash between the two would leave the map vouching for a hole.
        self.file.sync_all().await?;
        self.map.mark(index).await
    }

    /// How long chunk `index` is, or `None` if there is no such chunk.
    fn chunk_len(&self, index: u32) -> Option<u64> {
        if index >= self.plan.count() {
            return None;
        }
        let offset = u64::from(index) * self.plan.chunk_size();
        Some(self.plan.chunk_size().min(self.plan.size() - offset))
    }

    fn overrun(&self, index: u32, written: u64, expected: u64) -> Error {
        Error::Api {
            status: 206,
            message: format!("chunk {index} carried {written} bytes, expected {expected}"),
        }
    }

    /// Rename the partial onto `dest`, but only if every chunk is present.
    ///
    /// The check is the whole point of the type: without it a file with holes
    /// in the middle would be published as the real thing.
    pub(super) async fn finish(self, dest: &Path) -> Result<()> {
        if !self.map.is_complete() {
            return Err(Error::Api {
                status: 206,
                message: format!(
                    "refusing to finish {}: {} chunk(s) still missing",
                    dest.display(),
                    self.missing().len()
                ),
            });
        }
        drop(self.file);
        tokio::fs::rename(&self.path, dest).await?;
        // Only now: a crash before the rename must leave the map describing a
        // partial the next attempt can adopt whole.
        self.map.discard().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::chunks::Chunking;

    fn limits() -> Chunking {
        Chunking {
            min_size: 10,
            chunk_size: 10,
            max_chunks: 16,
        }
    }

    /// 25 bytes in 10-byte chunks: three chunks, the last one short.
    fn plan() -> ChunkPlan {
        ChunkPlan::new(25, limits())
    }

    fn body(bytes: &[u8]) -> reqwest::Response {
        reqwest::Response::from(
            http::Response::builder()
                .status(206)
                .body(bytes.to_vec())
                .unwrap(),
        )
    }

    /// The bytes chunk `index` of `content` holds.
    fn chunk_of(content: &[u8], index: u32) -> &[u8] {
        let start = (index as usize) * 10;
        &content[start..(start + 10).min(content.len())]
    }

    struct Fixture {
        dir: tempfile::TempDir,
        content: Vec<u8>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().unwrap(),
                content: (0..25).collect(),
            }
        }

        fn partial(&self) -> PathBuf {
            self.dir.path().join("a.txt.r1.dbsync-partial")
        }

        fn dest(&self) -> PathBuf {
            self.dir.path().join("a.txt")
        }

        async fn open(&self) -> Partial {
            Partial::open(&self.partial(), plan()).await.unwrap()
        }

        async fn put(&self, partial: &mut Partial, index: u32) {
            partial
                .write_chunk(index, &mut body(chunk_of(&self.content, index)))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_fresh_partial_wants_every_chunk() {
        let fixture = Fixture::new();
        assert_eq!(fixture.open().await.missing(), vec![0, 1, 2]);
    }

    /// The trap this type exists for: writing the last chunk first gives the
    /// partial its full length while the middle is still a hole.
    #[tokio::test]
    async fn a_full_length_partial_with_holes_is_not_finished() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;
        fixture.put(&mut partial, 2).await;

        assert_eq!(
            std::fs::metadata(fixture.partial()).unwrap().len(),
            25,
            "the partial should already be full length"
        );
        assert_eq!(partial.missing(), vec![0, 1]);
        assert!(partial.finish(&fixture.dest()).await.is_err());
        assert!(!fixture.dest().exists());
    }

    /// Chunks land at their true offsets, so any arrival order must produce
    /// exactly the same file.
    #[tokio::test]
    async fn chunks_arriving_out_of_order_rebuild_the_file_exactly() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;
        for index in [2, 0, 1] {
            fixture.put(&mut partial, index).await;
        }

        partial.finish(&fixture.dest()).await.unwrap();
        assert_eq!(std::fs::read(fixture.dest()).unwrap(), fixture.content);
        assert!(!fixture.partial().exists());
    }

    /// Nothing may be left beside the finished file, or the startup sweep
    /// would be cleaning up after every successful download.
    #[tokio::test]
    async fn finishing_clears_the_sidecar() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;
        for index in 0..3 {
            fixture.put(&mut partial, index).await;
        }
        partial.finish(&fixture.dest()).await.unwrap();

        assert!(
            !crate::api::chunkmap::sidecar_path(&fixture.partial()).exists(),
            "the sidecar outlived the download it described"
        );
    }

    /// The point of the sidecar: an interrupted download resumes with only the
    /// chunks it actually still needs.
    #[tokio::test]
    async fn a_reopened_partial_only_wants_what_is_missing() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;
        fixture.put(&mut partial, 0).await;
        fixture.put(&mut partial, 2).await;
        drop(partial);

        let mut resumed = fixture.open().await;
        assert_eq!(resumed.missing(), vec![1]);
        fixture.put(&mut resumed, 1).await;
        resumed.finish(&fixture.dest()).await.unwrap();
        assert_eq!(std::fs::read(fixture.dest()).unwrap(), fixture.content);
    }

    /// A truncated body would leave a hole *inside* a chunk the map then calls
    /// present, which no later check could ever catch.
    #[tokio::test]
    async fn a_short_chunk_body_is_rejected_and_not_marked() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;

        assert!(partial.write_chunk(0, &mut body(b"12345")).await.is_err());
        assert_eq!(partial.missing(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn an_overlong_chunk_body_is_rejected_and_not_marked() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;

        assert!(
            partial
                .write_chunk(1, &mut body(b"far too many bytes for one chunk"))
                .await
                .is_err()
        );
        assert!(partial.missing().contains(&1));
    }

    /// The last chunk is short by design; holding it to the nominal length
    /// would make every non-multiple file impossible to finish.
    #[tokio::test]
    async fn the_short_final_chunk_is_accepted_at_its_real_length() {
        let fixture = Fixture::new();
        let mut partial = fixture.open().await;
        fixture.put(&mut partial, 2).await;

        assert!(!partial.missing().contains(&2));
    }
}
