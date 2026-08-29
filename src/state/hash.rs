//! Dropbox's content hash.
//!
//! Dropbox identifies file content by a two-level SHA-256 tree, not by mtime:
//! split the file into 4 MiB blocks, SHA-256 each block, concatenate those raw
//! digests, and SHA-256 the concatenation. dbsync uses it to recognise the echo
//! of its own upload and avoid re-applying it (see `docs/architecture.md`).

use std::io::Read;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// Dropbox's fixed block size for the content hash.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Incremental content hasher, so a large file never has to be held in memory.
#[derive(Default)]
pub struct ContentHasher {
    /// Concatenated SHA-256 digests of each complete block seen so far.
    block_digests: Vec<u8>,
    /// Bytes of the block currently being filled.
    pending: Vec<u8>,
}

impl ContentHasher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed the next bytes of the file, in order.
    pub fn update(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let room = BLOCK_SIZE - self.pending.len();
            let take = room.min(data.len());
            self.pending.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.pending.len() == BLOCK_SIZE {
                self.flush_block();
            }
        }
    }

    /// Consume the hasher and return the hash as lowercase hex.
    pub fn finalize(mut self) -> String {
        if !self.pending.is_empty() {
            self.flush_block();
        }
        hex::encode(Sha256::digest(&self.block_digests))
    }

    fn flush_block(&mut self) {
        self.block_digests
            .extend_from_slice(&Sha256::digest(&self.pending));
        self.pending.clear();
    }
}

/// Compute the content hash of a file on disk, streaming it a block at a time.
pub fn hash_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path).map_err(|source| Error::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = ContentHasher::new();
    let mut buf = vec![0u8; BLOCK_SIZE];
    loop {
        let n = file.read(&mut buf).map_err(|source| Error::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

/// Compute the content hash of an in-memory buffer.
pub fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = ContentHasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty file hashes to SHA-256 of the empty string, since there are no
    /// block digests to concatenate.
    #[test]
    fn empty_input() {
        assert_eq!(
            hash_bytes(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// One short block: SHA-256 of the SHA-256 of the content.
    #[test]
    fn single_partial_block() {
        let expected = hex::encode(Sha256::digest(Sha256::digest(b"hello")));
        assert_eq!(hash_bytes(b"hello"), expected);
    }

    /// Exactly one full block must produce one digest, not two.
    #[test]
    fn exactly_one_block() {
        let data = vec![0x61u8; BLOCK_SIZE];
        let expected = hex::encode(Sha256::digest(Sha256::digest(&data)));
        assert_eq!(hash_bytes(&data), expected);
    }

    /// Crossing the block boundary must produce two block digests.
    #[test]
    fn spans_two_blocks() {
        let data = vec![0x62u8; BLOCK_SIZE + 1];
        let mut concat = Vec::new();
        concat.extend_from_slice(&Sha256::digest(&data[..BLOCK_SIZE]));
        concat.extend_from_slice(&Sha256::digest(&data[BLOCK_SIZE..]));
        assert_eq!(hash_bytes(&data), hex::encode(Sha256::digest(&concat)));
    }

    /// The result must not depend on how the input was chunked into `update`.
    #[test]
    fn chunking_does_not_change_the_result() {
        let data = vec![0x63u8; BLOCK_SIZE * 2 + 12345];
        let mut hasher = ContentHasher::new();
        for chunk in data.chunks(7919) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finalize(), hash_bytes(&data));
    }

    #[test]
    fn hashing_a_file_matches_hashing_its_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let data = vec![0x64u8; BLOCK_SIZE + 999];
        std::fs::write(&path, &data).unwrap();
        assert_eq!(hash_file(&path).unwrap(), hash_bytes(&data));
    }
}
