//! Which chunks of a partial download have actually landed.
//!
//! A single-stream download needs no such record: its progress *is* the length
//! of its partial file. Chunks arriving concurrently at their true offsets
//! break that equivalence — a partial can hold chunks 0 to 3 and 5, and no one
//! number expresses it. Worse, writing chunks at their offsets makes the file
//! reach its full length the moment the *last* chunk lands, so a length test
//! would call a hole-filled file complete.
//!
//! So progress moves into a sidecar beside the partial: a small header plus
//! one bit per chunk. Two rules make it trustworthy:
//!
//! - **The header must match.** It carries the size, chunk size and count the
//!   map was built for. If any differs from what the caller now expects, the
//!   map describes a different plan of a different revision and is discarded
//!   rather than reinterpreted — the bits would otherwise point at the wrong
//!   bytes.
//! - **A bit is durable before it counts.** Setting one rewrites and fsyncs
//!   the sidecar before the chunk is considered done. A crash may therefore
//!   lose progress and refetch a chunk, which is merely wasteful; the
//!   forbidden direction is claiming a chunk that is not on disk.
//!
//! The sidecar is named off the partial and shares its `.dbsync-partial`
//! prefix, so [`crate::reconcile::sweep`] clears the two together.

// Consumed once the download path fetches by chunk; the tests below are what
// hold the format to its promises meanwhile.
#![cfg_attr(not(test), allow(dead_code))]

use std::path::{Path, PathBuf};

use super::chunks::ChunkPlan;
use crate::error::Result;

/// Appended to a partial's name to give its sidecar. It *extends* that name
/// rather than replacing its suffix, so the startup sweep recognises both.
pub(super) const MAP_SUFFIX: &str = "-map";

/// Tags the sidecar and its version, so a format change is a mismatch rather
/// than a misreading of old bytes.
const MAGIC: &[u8; 8] = b"dbsyncM1";
/// `MAGIC`, then size, chunk size and count.
const HEADER_LEN: usize = 8 + 8 + 8 + 4;

/// The plan a map was written for. All of it must match to reuse the map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Header {
    size: u64,
    chunk_size: u64,
    count: u32,
}

impl Header {
    fn of(plan: ChunkPlan) -> Self {
        Self {
            size: plan.size(),
            chunk_size: plan.chunk_size(),
            count: plan.count(),
        }
    }

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.size.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_size.to_le_bytes());
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes
    }

    /// The header at the front of `bytes`, if it is one at all.
    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC {
            return None;
        }
        Some(Self {
            size: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
            chunk_size: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
            count: u32::from_le_bytes(bytes[24..28].try_into().ok()?),
        })
    }
}

/// How many bytes of bitmap `count` chunks need.
fn bitmap_len(count: u32) -> usize {
    count.div_ceil(8) as usize
}

/// The set of chunks already written to a partial, and the file recording it.
#[derive(Debug)]
pub(super) struct ChunkMap {
    path: PathBuf,
    header: Header,
    bits: Vec<u8>,
}

impl ChunkMap {
    /// The map for `plan` beside `partial`: the one on disk if it matches, an
    /// empty one otherwise.
    ///
    /// A mismatched, damaged or absent sidecar all mean the same thing — no
    /// chunk can be trusted — so all three start over rather than being told
    /// apart.
    pub(super) async fn open(partial: &Path, plan: ChunkPlan) -> Self {
        let path = sidecar_path(partial);
        let header = Header::of(plan);
        match tokio::fs::read(&path).await {
            Ok(bytes) => Self::decode(path, header, &bytes),
            Err(_) => Self::empty(path, header),
        }
    }

    fn decode(path: PathBuf, header: Header, bytes: &[u8]) -> Self {
        let matches = Header::decode(bytes) == Some(header);
        let bits = &bytes[HEADER_LEN.min(bytes.len())..];
        // A short bitmap is a torn write: the bits present may be sound, but
        // the ones missing would read as "not yet fetched" anyway, and telling
        // a truncated file from a sound one is not worth the risk.
        if !matches || bits.len() != bitmap_len(header.count) {
            return Self::empty(path, header);
        }
        Self {
            path,
            header,
            bits: bits.to_vec(),
        }
    }

    fn empty(path: PathBuf, header: Header) -> Self {
        Self {
            path,
            header,
            bits: vec![0; bitmap_len(header.count)],
        }
    }

    /// Has chunk `index` been written to the partial?
    pub(super) fn has(&self, index: u32) -> bool {
        let (byte, bit) = position(index);
        self.bits.get(byte).is_some_and(|bits| bits & bit != 0)
    }

    /// Is every chunk present — the only condition under which the partial may
    /// be renamed into place?
    pub(super) fn is_complete(&self) -> bool {
        (0..self.header.count).all(|index| self.has(index))
    }

    /// The chunks still to fetch.
    pub(super) fn missing(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.header.count).filter(|index| !self.has(*index))
    }

    /// Record that chunk `index` is on disk, durably.
    ///
    /// Returns only once the sidecar is fsynced, so a chunk is never claimed
    /// before the record of it survives a crash.
    pub(super) async fn mark(&mut self, index: u32) -> Result<()> {
        if index >= self.header.count {
            return Ok(());
        }
        let (byte, bit) = position(index);
        self.bits[byte] |= bit;
        self.flush().await
    }

    /// Rewrite the whole sidecar. It is a header and a handful of bytes, so
    /// there is nothing to gain from patching it in place.
    async fn flush(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&self.path).await?;
        let mut bytes = self.header.encode();
        bytes.extend_from_slice(&self.bits);
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Drop the sidecar, once the partial it describes is no longer one.
    pub(super) async fn discard(&self) {
        let _ = tokio::fs::remove_file(&self.path).await;
    }
}

/// Which byte and bit hold chunk `index`.
fn position(index: u32) -> (usize, u8) {
    ((index / 8) as usize, 1 << (index % 8))
}

/// Where the sidecar for `partial` lives.
///
/// It keeps the partial's whole name, so it sorts beside it and — since the
/// name still contains `.dbsync-partial` — the startup sweep recognises it.
pub(super) fn sidecar_path(partial: &Path) -> PathBuf {
    let mut name = partial.file_name().unwrap_or_default().to_os_string();
    name.push(MAP_SUFFIX);
    partial.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::chunks::Chunking;

    fn limits() -> Chunking {
        Chunking {
            min_size: 100,
            chunk_size: 100,
            max_chunks: 16,
        }
    }

    /// 300 bytes in 100-byte chunks: three chunks, one byte of bitmap.
    fn plan() -> ChunkPlan {
        ChunkPlan::new(300, limits())
    }

    fn partial(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().join("a.txt.r1.dbsync-partial")
    }

    #[tokio::test]
    async fn a_fresh_map_has_nothing_and_wants_everything() {
        let dir = tempfile::tempdir().unwrap();
        let map = ChunkMap::open(&partial(&dir), plan()).await;

        assert!(!map.is_complete());
        assert!(!map.has(0));
        assert_eq!(map.missing().collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    /// The point of the sidecar: progress that a length cannot express must
    /// survive a restart exactly as it was.
    #[tokio::test]
    async fn marked_chunks_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = ChunkMap::open(&partial(&dir), plan()).await;
        map.mark(0).await.unwrap();
        map.mark(2).await.unwrap();

        let reopened = ChunkMap::open(&partial(&dir), plan()).await;
        assert!(reopened.has(0) && !reopened.has(1) && reopened.has(2));
        assert_eq!(reopened.missing().collect::<Vec<_>>(), vec![1]);
        assert!(!reopened.is_complete());
    }

    #[tokio::test]
    async fn a_map_is_complete_only_once_every_chunk_is_in() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = ChunkMap::open(&partial(&dir), plan()).await;
        for index in [2, 0] {
            map.mark(index).await.unwrap();
            assert!(!map.is_complete());
        }
        map.mark(1).await.unwrap();
        assert!(map.is_complete());
    }

    /// A map from a different plan points its bits at different bytes, so
    /// reusing it would treat some other revision's chunks as this one's.
    #[tokio::test]
    async fn a_map_for_another_size_is_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = ChunkMap::open(&partial(&dir), plan()).await;
        map.mark(0).await.unwrap();

        let other = ChunkMap::open(&partial(&dir), ChunkPlan::new(500, limits())).await;
        assert!(!other.has(0));
    }

    #[tokio::test]
    async fn a_map_for_another_chunk_size_is_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = ChunkMap::open(&partial(&dir), plan()).await;
        map.mark(0).await.unwrap();

        let regrouped = Chunking {
            chunk_size: 150,
            ..limits()
        };
        let other = ChunkMap::open(&partial(&dir), ChunkPlan::new(300, regrouped)).await;
        assert!(!other.has(0));
    }

    /// A half-written sidecar from a crash must read as no progress, never as
    /// progress it cannot back up.
    #[tokio::test]
    async fn a_truncated_sidecar_is_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = ChunkMap::open(&partial(&dir), plan()).await;
        map.mark(0).await.unwrap();

        let sidecar = sidecar_path(&partial(&dir));
        let bytes = std::fs::read(&sidecar).unwrap();
        std::fs::write(&sidecar, &bytes[..bytes.len() - 1]).unwrap();

        assert!(!ChunkMap::open(&partial(&dir), plan()).await.has(0));
    }

    #[tokio::test]
    async fn a_sidecar_of_rubbish_is_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(sidecar_path(&partial(&dir)), b"not a chunk map at all").unwrap();

        let map = ChunkMap::open(&partial(&dir), plan()).await;
        assert!(!map.has(0));
        assert!(!map.is_complete());
    }

    #[tokio::test]
    async fn discarding_removes_the_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = ChunkMap::open(&partial(&dir), plan()).await;
        map.mark(0).await.unwrap();
        assert!(sidecar_path(&partial(&dir)).exists());

        map.discard().await;
        assert!(!sidecar_path(&partial(&dir)).exists());
    }

    /// The sweep finds leftovers by name, so a sidecar that does not look like
    /// a partial would survive every restart forever.
    #[test]
    fn a_sidecar_is_swept_like_the_partial_it_belongs_to() {
        let partial = Path::new("/tmp/a.txt.r1.dbsync-partial");
        let sidecar = sidecar_path(partial);
        assert_eq!(sidecar.parent(), partial.parent());
        assert!(crate::api::is_partial(&sidecar));
    }

    #[test]
    fn a_header_round_trips() {
        let header = Header::of(plan());
        assert_eq!(Header::decode(&header.encode()), Some(header));
    }

    #[test]
    fn a_bitmap_is_one_bit_per_chunk() {
        assert_eq!(bitmap_len(1), 1);
        assert_eq!(bitmap_len(8), 1);
        assert_eq!(bitmap_len(9), 2);
        assert_eq!(bitmap_len(64), 8);
    }

    /// Bits must not collide across a byte boundary, or marking one chunk
    /// would silently claim another that was never fetched.
    #[tokio::test]
    async fn chunks_past_the_first_byte_get_their_own_bits() {
        let dir = tempfile::tempdir().unwrap();
        let wide = ChunkPlan::new(1_200, limits());
        assert_eq!(wide.count(), 12);
        let mut map = ChunkMap::open(&partial(&dir), wide).await;
        map.mark(8).await.unwrap();

        assert!(map.has(8));
        assert!(!map.has(0));
        assert_eq!(
            map.missing().collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11]
        );
    }
}
