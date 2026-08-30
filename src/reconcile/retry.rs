//! Re-attempting entries that failed earlier.
//!
//! A failed download leaves the file *silently absent locally*: the pull
//! reports success, the cursor has moved past it, and nothing will re-deliver
//! the path until it happens to change remotely. Recording the failure
//! (`crate::state::failures`) is what makes it visible; this is what makes it
//! self-healing.
//!
//! The pass is deliberately modest. It looks each path up individually rather
//! than re-listing, because the page it came from is long consumed and a
//! Dropbox cursor cannot be rewound. It runs after a pull, so a transient
//! network problem gets a second chance within the same session.

use crate::api::RemoteEntry;
use crate::error::Result;
use crate::state::SyncState;

/// What a retry pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Retried {
    /// How many paths were attempted.
    pub attempted: usize,
    /// How many are now on disk and no longer recorded as missing.
    pub recovered: usize,
    /// How many were found to be gone remotely, so there is nothing to fetch.
    pub vanished: usize,
}

/// The paths worth another attempt, oldest failure first.
///
/// Only transient failures are returned: re-requesting a path the local
/// filesystem cannot represent fails identically every time, and would bury the
/// recoverable entries in noise. Ordering by first sighting means the
/// longest-broken entry is tried first, so one persistently failing path cannot
/// starve the others of attempts.
pub fn candidates(state: &SyncState) -> Vec<String> {
    let mut failures: Vec<_> = state.retryable_failures().collect();
    failures.sort_by_key(|failure| failure.first_seen);
    failures
        .into_iter()
        .map(|failure| failure.display_path.clone())
        .collect()
}

/// Whether a looked-up entry means the path is gone rather than fetchable.
///
/// A tombstone is a *resolution*, not a failure: the file was deleted remotely
/// between the failed download and the retry, so there is nothing to recover
/// and the record should be dropped rather than retried forever.
pub fn is_gone(entry: &RemoteEntry) -> bool {
    matches!(entry, RemoteEntry::Deleted(_))
}

/// Fold one lookup outcome into the state, returning how it should be counted.
///
/// A lookup that itself fails is left recorded: it stays on the list and is
/// tried again next pass, which is the whole point of the list surviving.
pub fn resolve(state: &mut SyncState, path: &str, looked_up: &Result<RemoteEntry>) -> Outcome {
    match looked_up {
        Ok(entry) if is_gone(entry) => {
            state.clear_failure(path);
            Outcome::Vanished
        }
        Ok(_) => Outcome::Fetchable,
        Err(error) => {
            state.record_failure(path, error);
            Outcome::StillFailing
        }
    }
}

/// What resolving one retry candidate concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The entry exists and should be downloaded.
    Fetchable,
    /// The entry is gone remotely; the failure record was dropped.
    Vanished,
    /// The lookup failed; the record stands and will be retried again.
    StillFailing,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{RemoteDeleted, RemoteFile};
    use crate::error::Error;

    fn file(path: &str) -> RemoteEntry {
        RemoteEntry::File(RemoteFile {
            path_lower: path.to_lowercase(),
            path_display: path.into(),
            rev: "r1".into(),
            size: 1,
            content_hash: None,
        })
    }

    fn state_with(path: &str, error: Error) -> SyncState {
        let mut state = SyncState::new();
        state.record_failure(path, &error);
        state
    }

    /// A path the filesystem cannot represent is not worth a request: it will
    /// fail the same way every pass and drown out what is recoverable.
    #[test]
    fn permanent_failures_are_not_candidates() {
        let state = state_with("/long", Error::Io(std::io::Error::from_raw_os_error(36)));
        assert!(candidates(&state).is_empty());
    }

    #[test]
    fn transient_failures_are_candidates() {
        let state = state_with("/a.txt", Error::Config("flaky".into()));
        assert_eq!(candidates(&state), vec!["/a.txt".to_string()]);
    }

    /// A file deleted remotely between the failure and the retry is resolved,
    /// not retried: there is nothing left to fetch.
    #[test]
    fn a_vanished_path_stops_being_recorded() {
        let mut state = state_with("/gone.txt", Error::Config("flaky".into()));
        let looked_up = Ok(RemoteEntry::Deleted(RemoteDeleted {
            path_lower: "/gone.txt".into(),
            path_display: Some("/gone.txt".into()),
        }));

        assert_eq!(
            resolve(&mut state, "/gone.txt", &looked_up),
            Outcome::Vanished
        );
        assert_eq!(state.failure_count(), 0);
    }

    /// A lookup that fails must leave the record standing, or the path would
    /// drop off the list and be silently missing again.
    #[test]
    fn a_failed_lookup_keeps_the_record() {
        let mut state = state_with("/a.txt", Error::Config("flaky".into()));
        let looked_up: Result<RemoteEntry> = Err(Error::Config("still flaky".into()));

        assert_eq!(
            resolve(&mut state, "/a.txt", &looked_up),
            Outcome::StillFailing
        );
        assert_eq!(state.failure_count(), 1);
        let failure = state.failures().next().unwrap();
        assert_eq!(failure.attempts, 2, "the attempt counted");
    }

    /// A path that still exists is handed on to the downloader; the record is
    /// cleared by the apply path on success, not here.
    #[test]
    fn an_existing_path_is_fetchable() {
        let mut state = state_with("/a.txt", Error::Config("flaky".into()));
        let looked_up = Ok(file("/a.txt"));

        assert_eq!(
            resolve(&mut state, "/a.txt", &looked_up),
            Outcome::Fetchable
        );
        assert_eq!(state.failure_count(), 1, "cleared on a successful apply");
    }

    /// The longest-broken entry goes first, so one bad path cannot starve the
    /// rest of their attempts.
    #[test]
    fn candidates_are_oldest_first() {
        let mut state = SyncState::new();
        state.record_failure("/second.txt", &Error::Config("b".into()));
        // Force a distinguishable first_seen without sleeping a whole second.
        let mut older = state.failures().next().unwrap().clone();
        older.display_path = "/first.txt".into();
        older.first_seen -= 100;
        state.insert_failure("/first.txt", older);

        assert_eq!(
            candidates(&state),
            vec!["/first.txt".to_string(), "/second.txt".to_string()]
        );
    }
}
