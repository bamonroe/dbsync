//! Running blocking work somewhere other than a runtime worker.
//!
//! Most of dbsync's disk work is small enough to do inline, but three jobs are
//! not: hashing a file (a multi-gigabyte read that pins a core for its whole
//! duration), saving the state (two `fsync`s, which park the thread for as long
//! as the disk feels like), and walking the synced tree. Doing any of those on
//! a runtime thread stalls every other task that thread is driving — including
//! the downloads and the long-poll the daemon exists to service.
//!
//! Each of those has an `async` twin next to the blocking original — see
//! [`crate::state::hash::hash_file_off_thread`],
//! [`crate::state::StateDb::save_off_thread`] and
//! [`crate::reconcile::sweep::partial_downloads`] — and every one of them is
//! this function underneath.

use crate::error::{Error, Result};

/// Run `work` on tokio's blocking pool and await its result.
///
/// The closure owns everything it touches, which is what lets it move to
/// another thread: callers clone the handful of paths involved rather than
/// borrowing across the `await`.
pub async fn run<T, F>(work: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| Error::Blocking(error.to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_value_comes_back_from_the_other_thread() {
        assert_eq!(run(|| Ok(2 + 2)).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn the_closures_error_is_the_callers_error() {
        let failed: Result<()> = run(|| Err(Error::Config("nope".into()))).await;
        assert!(matches!(failed, Err(Error::Config(_))));
    }

    /// A panicked job must surface as an error, not take the caller down with
    /// it: one unreadable file should not end the daemon.
    #[tokio::test]
    async fn a_panic_becomes_an_error() {
        let panicked: Result<()> = run(|| panic!("boom")).await;
        assert!(matches!(panicked, Err(Error::Blocking(_))));
    }
}
