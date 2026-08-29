//! Coalescing bursts of filesystem events into one change per path.
//!
//! A single editor save is rarely a single inotify event: it is often a create,
//! several writes, a rename, and a chmod. Uploading each of those would be
//! wasteful and would race with itself, so a path is held until it has been
//! quiet for the debounce window, and repeated events push that deadline back.
//!
//! The clock is a parameter rather than read from inside, which is what makes
//! this testable without sleeping.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio::time::Instant;

/// Accumulates touched paths and releases them once they go quiet.
#[derive(Debug)]
pub struct Debouncer {
    quiet: Duration,
    /// Path → the moment it becomes eligible to release.
    pending: HashMap<PathBuf, Instant>,
}

impl Debouncer {
    pub fn new(quiet: Duration) -> Self {
        Self {
            quiet,
            pending: HashMap::new(),
        }
    }

    /// Record that `path` changed, restarting its quiet period.
    pub fn note(&mut self, path: PathBuf, now: Instant) {
        self.pending.insert(path, now + self.quiet);
    }

    /// When the earliest pending path comes due, if anything is pending.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().min().copied()
    }

    /// Take every path that has been quiet long enough.
    ///
    /// Sorted, so a batch is applied in a stable order — parents before the
    /// children whose paths extend them.
    pub fn take_ready(&mut self, now: Instant) -> Vec<PathBuf> {
        let ready: Vec<PathBuf> = self
            .pending
            .iter()
            .filter(|(_, due)| **due <= now)
            .map(|(path, _)| path.clone())
            .collect();
        for path in &ready {
            self.pending.remove(path);
        }
        let mut ready = ready;
        ready.sort();
        ready
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUIET: Duration = Duration::from_millis(500);

    fn debouncer() -> (Debouncer, Instant) {
        (Debouncer::new(QUIET), Instant::now())
    }

    #[tokio::test(start_paused = true)]
    async fn a_path_is_held_for_the_quiet_period() {
        let (mut debouncer, start) = debouncer();
        debouncer.note("/a".into(), start);

        assert!(debouncer.take_ready(start).is_empty());
        assert_eq!(
            debouncer.take_ready(start + QUIET),
            vec![PathBuf::from("/a")]
        );
    }

    /// The point of the whole module: a burst of writes is one upload.
    #[tokio::test(start_paused = true)]
    async fn repeated_events_collapse_into_one_release() {
        let (mut debouncer, start) = debouncer();
        for step in 0..5 {
            debouncer.note("/a".into(), start + Duration::from_millis(100 * step));
        }

        let ready = debouncer.take_ready(start + Duration::from_millis(400) + QUIET);
        assert_eq!(ready, vec![PathBuf::from("/a")]);
        assert!(debouncer.is_empty());
    }

    /// A still-active file must not be released mid-burst.
    #[tokio::test(start_paused = true)]
    async fn a_later_event_pushes_the_deadline_back() {
        let (mut debouncer, start) = debouncer();
        debouncer.note("/a".into(), start);
        debouncer.note("/a".into(), start + Duration::from_millis(400));

        assert!(debouncer.take_ready(start + QUIET).is_empty());
        assert!(
            !debouncer
                .take_ready(start + Duration::from_millis(900))
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn paths_are_tracked_independently() {
        let (mut debouncer, start) = debouncer();
        debouncer.note("/a".into(), start);
        debouncer.note("/b".into(), start + Duration::from_millis(300));

        assert_eq!(
            debouncer.take_ready(start + QUIET),
            vec![PathBuf::from("/a")]
        );
        assert_eq!(
            debouncer.take_ready(start + Duration::from_millis(800)),
            vec![PathBuf::from("/b")]
        );
    }

    /// A batch must be ordered, so a directory is handled before its contents.
    #[tokio::test(start_paused = true)]
    async fn a_batch_comes_out_sorted() {
        let (mut debouncer, start) = debouncer();
        debouncer.note("/b/z".into(), start);
        debouncer.note("/a".into(), start);
        debouncer.note("/b".into(), start);

        assert_eq!(
            debouncer.take_ready(start + QUIET),
            vec![
                PathBuf::from("/a"),
                PathBuf::from("/b"),
                PathBuf::from("/b/z")
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_is_the_earliest_pending_one() {
        let (mut debouncer, start) = debouncer();
        assert!(debouncer.next_deadline().is_none());

        debouncer.note("/late".into(), start + Duration::from_millis(300));
        debouncer.note("/early".into(), start);
        assert_eq!(debouncer.next_deadline(), Some(start + QUIET));
    }
}
