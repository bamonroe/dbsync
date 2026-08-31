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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::chunkmap::ChunkMap;
use super::chunks::ChunkPlan;
use crate::error::{Error, Result};

/// How much of a body to gather before it goes to the disk.
///
/// Network frames arrive ~8-16 KiB at a time, and handing each one to the
/// blocking pool on its own spends a thread dispatch on a write far smaller
/// than the disk cares about. Gathering them into 256 KiB blocks cuts that
/// traffic by roughly 16x for the same bytes.
pub(super) const WRITE_BUFFER_BYTES: usize = 256 * 1024;

/// A partial download in progress: the file, its plan, and what has landed.
///
/// Shared by reference across concurrent chunk writes, one open handle between
/// them. Sharing a handle is only safe because nothing here uses its cursor:
/// every write names its own absolute offset, so two chunks in flight cannot
/// move each other's bytes. Only the map is mutable shared state, and it is
/// held just long enough to set a bit.
pub(super) struct Partial {
    path: PathBuf,
    plan: ChunkPlan,
    file: Arc<std::fs::File>,
    map: tokio::sync::Mutex<ChunkMap>,
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
            file: Arc::new(file.into_std().await),
            map: tokio::sync::Mutex::new(map),
        })
    }

    /// The chunks still to fetch. Empty means the download is done.
    pub(super) async fn missing(&self) -> Vec<u32> {
        self.map.lock().await.missing().collect()
    }

    /// Stream one chunk's body into its slot and record it.
    ///
    /// The body must be exactly as long as the chunk: a short one would leave
    /// a hole inside a chunk the map then calls present, which no later check
    /// could detect.
    pub(super) async fn write_chunk(
        &self,
        index: u32,
        response: &mut reqwest::Response,
    ) -> Result<()> {
        let Some(expected) = self.chunk_len(index) else {
            return Ok(());
        };
        let mut writer = ChunkWriter::new(
            Arc::clone(&self.file),
            u64::from(index) * self.plan.chunk_size(),
        );

        let mut written = 0_u64;
        while let Some(bytes) = response.chunk().await? {
            written += bytes.len() as u64;
            if written > expected {
                return Err(overrun(index, written, expected));
            }
            writer.push(&bytes).await?;
        }
        if written != expected {
            return Err(overrun(index, written, expected));
        }
        writer.finish().await?;
        self.map.lock().await.mark(index).await
    }

    /// How long chunk `index` is, or `None` if there is no such chunk.
    fn chunk_len(&self, index: u32) -> Option<u64> {
        if index >= self.plan.count() {
            return None;
        }
        let offset = u64::from(index) * self.plan.chunk_size();
        Some(self.plan.chunk_size().min(self.plan.size() - offset))
    }

    /// Rename the partial onto `dest`, but only if every chunk is present.
    ///
    /// The check is the whole point of the type: without it a file with holes
    /// in the middle would be published as the real thing.
    pub(super) async fn finish(self, dest: &Path) -> Result<()> {
        let map = self.map.into_inner();
        if !map.is_complete() {
            return Err(Error::Api {
                status: 206,
                message: format!(
                    "refusing to finish {}: {} chunk(s) still missing",
                    dest.display(),
                    map.missing().count()
                ),
            });
        }
        tokio::fs::rename(&self.path, dest).await?;
        // Only now: a crash before the rename must leave the map describing a
        // partial the next attempt can adopt whole.
        map.discard().await;
        Ok(())
    }

    /// Throw the partial and its map away, so the next attempt starts clean.
    ///
    /// For when the revision itself turns out not to be what the plan assumed
    /// — a resumed map cannot be salvaged in that case, only discarded.
    pub(super) async fn abandon(self) {
        self.map.into_inner().discard().await;
        let _ = tokio::fs::remove_file(&self.path).await;
    }
}

/// Gathers one chunk's body in memory and writes it out in
/// [`WRITE_BUFFER_BYTES`] blocks at explicit offsets.
///
/// The positional writes are what let all the chunks share a single handle:
/// nothing reads or moves the file's cursor, so the order the blocks go out in
/// has no bearing on where they land.
struct ChunkWriter {
    file: Arc<std::fs::File>,
    /// Where the next block belongs — the chunk's start, advanced by what has
    /// already been written.
    offset: u64,
    buffer: Vec<u8>,
}

impl ChunkWriter {
    fn new(file: Arc<std::fs::File>, offset: u64) -> Self {
        Self {
            file,
            offset,
            buffer: Vec::with_capacity(WRITE_BUFFER_BYTES),
        }
    }

    /// Take one network frame, spilling once a full block has gathered.
    async fn push(&mut self, bytes: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() >= WRITE_BUFFER_BYTES {
            self.spill().await?;
        }
        Ok(())
    }

    /// Write what has gathered so far at its offset.
    ///
    /// The emptied buffer comes back from the blocking pool rather than being
    /// dropped there, so one allocation serves the whole chunk.
    async fn spill(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = Arc::clone(&self.file);
        let offset = self.offset;
        let mut buffer = std::mem::take(&mut self.buffer);
        self.offset += buffer.len() as u64;
        self.buffer = crate::blocking::run(move || {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&buffer, offset)?;
            buffer.clear();
            Ok(buffer)
        })
        .await?;
        Ok(())
    }

    /// Spill the tail and put the bytes on the platter.
    ///
    /// The sync is not optional: the bytes must be on disk before the bit that
    /// claims them is, or a crash between the two would leave the map vouching
    /// for a hole.
    async fn finish(mut self) -> Result<()> {
        self.spill().await?;
        crate::blocking::run(move || Ok(self.file.sync_all()?)).await
    }
}

fn overrun(index: u32, written: u64, expected: u64) -> Error {
    Error::Api {
        status: 206,
        message: format!("chunk {index} carried {written} bytes, expected {expected}"),
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
            ..Chunking::default()
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

        async fn put(&self, partial: &Partial, index: u32) {
            partial
                .write_chunk(index, &mut body(chunk_of(&self.content, index)))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_fresh_partial_wants_every_chunk() {
        let fixture = Fixture::new();
        assert_eq!(fixture.open().await.missing().await, vec![0, 1, 2]);
    }

    /// The trap this type exists for: writing the last chunk first gives the
    /// partial its full length while the middle is still a hole.
    #[tokio::test]
    async fn a_full_length_partial_with_holes_is_not_finished() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;
        fixture.put(&partial, 2).await;

        assert_eq!(
            std::fs::metadata(fixture.partial()).unwrap().len(),
            25,
            "the partial should already be full length"
        );
        assert_eq!(partial.missing().await, vec![0, 1]);
        assert!(partial.finish(&fixture.dest()).await.is_err());
        assert!(!fixture.dest().exists());
    }

    /// Chunks land at their true offsets, so any arrival order must produce
    /// exactly the same file.
    #[tokio::test]
    async fn chunks_arriving_out_of_order_rebuild_the_file_exactly() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;
        for index in [2, 0, 1] {
            fixture.put(&partial, index).await;
        }

        partial.finish(&fixture.dest()).await.unwrap();
        assert_eq!(std::fs::read(fixture.dest()).unwrap(), fixture.content);
        assert!(!fixture.partial().exists());
    }

    /// Concurrent chunks must not share a file cursor: with one handle between
    /// them, a seek by one lands the other's bytes at the wrong offset and the
    /// file is quietly scrambled.
    #[tokio::test]
    async fn concurrent_chunks_land_at_their_own_offsets() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;

        futures_util::future::join_all((0..3).map(|index| fixture.put(&partial, index))).await;

        partial.finish(&fixture.dest()).await.unwrap();
        assert_eq!(std::fs::read(fixture.dest()).unwrap(), fixture.content);
    }

    /// Nothing may be left beside the finished file, or the startup sweep
    /// would be cleaning up after every successful download.
    #[tokio::test]
    async fn finishing_clears_the_sidecar() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;
        for index in 0..3 {
            fixture.put(&partial, index).await;
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
        let partial = fixture.open().await;
        fixture.put(&partial, 0).await;
        fixture.put(&partial, 2).await;
        drop(partial);

        let resumed = fixture.open().await;
        assert_eq!(resumed.missing().await, vec![1]);
        fixture.put(&resumed, 1).await;
        resumed.finish(&fixture.dest()).await.unwrap();
        assert_eq!(std::fs::read(fixture.dest()).unwrap(), fixture.content);
    }

    /// A truncated body would leave a hole *inside* a chunk the map then calls
    /// present, which no later check could ever catch.
    #[tokio::test]
    async fn a_short_chunk_body_is_rejected_and_not_marked() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;

        assert!(partial.write_chunk(0, &mut body(b"12345")).await.is_err());
        assert_eq!(partial.missing().await, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn an_overlong_chunk_body_is_rejected_and_not_marked() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;

        assert!(
            partial
                .write_chunk(1, &mut body(b"far too many bytes for one chunk"))
                .await
                .is_err()
        );
        assert!(partial.missing().await.contains(&1));
    }

    /// A chunk bigger than one write block goes out in several pieces, and
    /// each has to pick up where the last left off — an offset that failed to
    /// advance would overwrite the chunk's own head with its tail.
    #[tokio::test]
    async fn a_chunk_spanning_several_write_blocks_lands_intact() {
        let chunk_size = (WRITE_BUFFER_BYTES + 1_000) as u64;
        let limits = Chunking {
            min_size: 10,
            chunk_size,
            max_chunks: 16,
            ..Chunking::default()
        };
        let content: Vec<u8> = (0..chunk_size * 2).map(|i| (i % 251) as u8).collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.bin.r1.dbsync-partial");
        let dest = dir.path().join("big.bin");

        let partial = Partial::open(&path, ChunkPlan::new(content.len() as u64, limits))
            .await
            .unwrap();
        // Second chunk first: its bytes must not be reachable from chunk 0's
        // spills, whichever order the blocks go out in.
        for index in [1, 0] {
            let start = (index as usize) * chunk_size as usize;
            let bytes = &content[start..start + chunk_size as usize];
            partial.write_chunk(index, &mut body(bytes)).await.unwrap();
        }

        partial.finish(&dest).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), content);
    }

    /// The last chunk is short by design; holding it to the nominal length
    /// would make every non-multiple file impossible to finish.
    #[tokio::test]
    async fn the_short_final_chunk_is_accepted_at_its_real_length() {
        let fixture = Fixture::new();
        let partial = fixture.open().await;
        fixture.put(&partial, 2).await;

        assert!(!partial.missing().await.contains(&2));
    }
}
