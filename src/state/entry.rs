//! What dbsync remembers about one synced file.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// The last-known agreed state of a single file, local and remote.
///
/// `rev` and `content_hash` come from Dropbox; `mtime` and `size` describe the
/// local file as it stood when we last reconciled it. Together they answer the
/// two questions the reconciler asks: "did the remote move on?" and "did the
/// user touch this locally?"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncEntry {
    /// Dropbox's opaque revision id for the file.
    pub rev: String,
    /// Dropbox content hash, the identity of the bytes. See [`super::hash`].
    pub content_hash: String,
    /// Local modification time, as whole nanoseconds since the Unix epoch.
    pub mtime_nanos: u128,
    /// Size in bytes.
    pub size: u64,
    /// The path as Dropbox displays it, preserving case.
    pub display_path: String,
}

impl SyncEntry {
    /// True when the local file's cheap metadata still matches what we
    /// recorded.
    ///
    /// This is a fast pre-filter, not proof: an editor can rewrite a file to
    /// the same size within the same timestamp granularity. When it returns
    /// false the reconciler must hash to find out what really happened; when it
    /// returns true, hashing is skipped, which is what keeps a large tree cheap
    /// to scan.
    pub fn metadata_matches(&self, size: u64, mtime: SystemTime) -> bool {
        self.size == size && self.mtime_nanos == to_nanos(mtime)
    }

    /// True when Dropbox is reporting content we already have.
    pub fn matches_remote(&self, rev: &str, content_hash: &str) -> bool {
        self.rev == rev || self.content_hash == content_hash
    }
}

/// Convert a [`SystemTime`] to nanoseconds since the Unix epoch.
///
/// Times before the epoch clamp to zero: they cannot come from a real sync, and
/// clamping keeps the stored value a plain unsigned integer.
pub fn to_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
}

/// Convert nanoseconds since the Unix epoch back to a [`SystemTime`].
pub fn from_nanos(nanos: u128) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> SyncEntry {
        SyncEntry {
            rev: "0123".into(),
            content_hash: "abcd".into(),
            mtime_nanos: 1_700_000_000_000_000_000,
            size: 42,
            display_path: "/Photos/Cat.JPG".into(),
        }
    }

    #[test]
    fn identical_metadata_matches() {
        let e = entry();
        assert!(e.metadata_matches(42, from_nanos(e.mtime_nanos)));
    }

    #[test]
    fn a_changed_size_does_not_match() {
        let e = entry();
        assert!(!e.metadata_matches(43, from_nanos(e.mtime_nanos)));
    }

    #[test]
    fn a_changed_mtime_does_not_match() {
        let e = entry();
        assert!(!e.metadata_matches(42, from_nanos(e.mtime_nanos + 1)));
    }

    /// The echo of our own upload comes back with a new rev but the same
    /// content hash, and must not be re-applied.
    #[test]
    fn the_same_content_under_a_new_rev_is_not_a_remote_change() {
        assert!(entry().matches_remote("9999", "abcd"));
    }

    #[test]
    fn different_rev_and_hash_is_a_real_remote_change() {
        assert!(!entry().matches_remote("9999", "eeee"));
    }

    #[test]
    fn nanosecond_conversion_round_trips() {
        let now = SystemTime::now();
        assert_eq!(to_nanos(from_nanos(to_nanos(now))), to_nanos(now));
    }

    /// A pre-epoch mtime is nonsense for a synced file; clamp rather than panic.
    #[test]
    fn pre_epoch_times_clamp_to_zero() {
        assert_eq!(to_nanos(UNIX_EPOCH - Duration::from_secs(10)), 0);
    }
}
