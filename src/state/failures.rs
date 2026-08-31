//! Entries that could not be applied, remembered rather than only logged.
//!
//! A warning in a log is not a record: it scrolls away, it is gone when the
//! container is recreated, and nothing can be asked "what is missing?". A file
//! that failed to download is *silently absent locally* — the sync looks
//! finished and the file simply is not there. That is the one failure mode this
//! module exists to make impossible.
//!
//! The record is deliberately part of [`super::SyncState`], so it is written by
//! the same atomic rename as everything else and survives a restart.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Why an entry could not be applied, and whether it is worth trying again.
///
/// The distinction is the point: a dropped connection is bad luck and should be
/// retried on the next pass, while a path the filesystem cannot represent will
/// fail identically forever. Retrying the second kind wastes a request every
/// pass and, worse, buries the first kind in noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// Worth another attempt: a network error, a timeout, a server-side 5xx.
    Transient,
    /// Will not succeed by being repeated: the local filesystem refuses this
    /// path, so only a rename or a config change can fix it.
    Permanent,
}

impl FailureKind {
    /// Whether a retry pass should pick this up.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Transient)
    }
}

/// Which way the failed transfer was going.
///
/// A retry pass has to know: re-fetching a path that failed to *upload* would
/// pull the remote copy over the local edit that never got sent, which is the
/// one outcome worse than the failure itself. The two directions are therefore
/// retried by their own halves of the reconciler and never crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Remote to local: the file is missing from disk.
    ///
    /// The default, so a state file written before directions were recorded
    /// loads as what it could only have held: download failures.
    #[default]
    Download,
    /// Local to remote: the local edit never reached Dropbox.
    Upload,
}

impl Direction {
    /// How to name this direction to an operator.
    pub fn label(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Upload => "upload",
        }
    }

    /// The inverse of [`label`](Self::label): the direction an operator named,
    /// or `None` if it names neither. Kept beside `label` so the two spellings
    /// cannot drift apart.
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "download" => Some(Self::Download),
            "upload" => Some(Self::Upload),
            _ => None,
        }
    }
}

/// One entry that failed, and what is known about why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    /// The remote path, in its original casing, for showing to a human.
    pub display_path: String,
    /// The most recent error, rendered. Stored as text because the point is to
    /// show it to an operator, not to match on it.
    pub error: String,
    pub kind: FailureKind,
    /// Which way the transfer was going, so the right half of the reconciler
    /// retries it. Defaulted on load for state files written before it existed.
    #[serde(default)]
    pub direction: Direction,
    /// How many times this path has now failed. A climbing count on a
    /// "transient" error is how a misclassification shows itself.
    pub attempts: u32,
    /// Unix seconds of the first and most recent failure. Seconds, not a
    /// formatted date: the file is machine-written and rendered on the way out.
    pub first_seen: u64,
    pub last_seen: u64,
}

impl Failure {
    /// Record a first failure for a path.
    pub fn new(
        display_path: impl Into<String>,
        error: impl Into<String>,
        kind: FailureKind,
        direction: Direction,
    ) -> Self {
        let now = unix_seconds();
        Self {
            display_path: display_path.into(),
            error: error.into(),
            kind,
            direction,
            attempts: 1,
            first_seen: now,
            last_seen: now,
        }
    }

    /// Fold a repeat failure into an existing record.
    ///
    /// The newest error and kind win — an entry can start out transient and be
    /// reclassified — but `first_seen` is kept, because how long something has
    /// been broken is the useful part.
    pub fn record_again(
        &mut self,
        error: impl Into<String>,
        kind: FailureKind,
        direction: Direction,
    ) {
        self.error = error.into();
        self.kind = kind;
        self.direction = direction;
        self.attempts = self.attempts.saturating_add(1);
        self.last_seen = unix_seconds();
    }
}

/// Seconds since the epoch, or 0 if the clock is before it.
fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Classify an error by whether repeating the operation could ever help.
///
/// Deliberately conservative: anything not recognised as permanent is treated
/// as transient. A needless retry costs one request, while wrongly calling
/// something permanent means a file is never fetched again.
pub fn classify(error: &crate::error::Error) -> FailureKind {
    if is_unrepresentable_path(error) {
        FailureKind::Permanent
    } else {
        FailureKind::Transient
    }
}

/// Whether the error is the filesystem refusing the path itself.
///
/// `ENAMETOOLONG` is the one seen in practice: Dropbox allows names longer than
/// Linux's 255-byte limit, so a legal remote path can have no legal local name.
fn is_unrepresentable_path(error: &crate::error::Error) -> bool {
    use crate::error::Error;
    let source = match error {
        Error::ReadFile { source, .. } | Error::Io(source) => source,
        _ => return false,
    };
    source.raw_os_error() == Some(ENAMETOOLONG)
}

/// `ENAMETOOLONG`. Named here rather than pulled in as a `libc` dependency for
/// one integer that has been stable on Linux for decades.
const ENAMETOOLONG: i32 = 36;

#[cfg(test)]
mod tests {
    use super::*;

    /// The retry-request file is written by an operator using the same words
    /// the status output prints, so the two spellings have to agree.
    #[test]
    fn every_label_parses_back_to_its_direction() {
        for direction in [Direction::Download, Direction::Upload] {
            assert_eq!(Direction::from_label(direction.label()), Some(direction));
        }
        assert_eq!(Direction::from_label("sideways"), None);
    }

    #[test]
    fn a_repeat_failure_keeps_the_first_sighting_and_counts_up() {
        let mut failure = Failure::new(
            "/a.txt",
            "boom",
            FailureKind::Transient,
            Direction::Download,
        );
        let first = failure.first_seen;
        failure.record_again("worse", FailureKind::Permanent, Direction::Download);

        assert_eq!(failure.attempts, 2);
        assert_eq!(failure.error, "worse");
        assert_eq!(failure.kind, FailureKind::Permanent, "the newest kind wins");
        assert_eq!(failure.first_seen, first, "how long it has been broken");
    }

    #[test]
    fn only_transient_failures_are_retried() {
        assert!(FailureKind::Transient.is_retryable());
        assert!(!FailureKind::Permanent.is_retryable());
    }

    /// A path the filesystem cannot represent will fail the same way forever,
    /// so retrying it every pass is pure waste.
    #[test]
    fn a_name_too_long_is_permanent() {
        let error = crate::error::Error::Io(std::io::Error::from_raw_os_error(ENAMETOOLONG));
        assert_eq!(classify(&error), FailureKind::Permanent);
    }

    /// Anything unrecognised is retried: a wasted request is cheaper than a
    /// file that is never fetched again.
    #[test]
    fn an_unrecognised_error_is_transient() {
        let error = crate::error::Error::Config("nothing to do with paths".into());
        assert_eq!(classify(&error), FailureKind::Transient);

        let io = crate::error::Error::Io(std::io::Error::from_raw_os_error(28)); // ENOSPC
        assert_eq!(classify(&io), FailureKind::Transient, "disk full may clear");
    }
}
