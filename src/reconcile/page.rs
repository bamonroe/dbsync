//! Applying one page of entries, with the downloads running concurrently.
//!
//! The page is first cut into steps by [`schedule::partition`]: serial work
//! (tombstones, folders) and groups of file downloads that may overlap. Only
//! the [`apply::fetch`] phase of a download is concurrent. [`apply::decide`]
//! runs before the fan-out and [`apply::record`] after it, both against the one
//! `&mut SyncState` this module owns, so the state is never mutated from two
//! places at once — that is exactly what the three-phase split in
//! [`apply`](super::apply) buys.
//!
//! **Outcomes are recorded in listing order, not completion order.** Downloads
//! finish out of order by nature, and recording as they land would make the
//! state file depend on the timing of the network — two runs over one page
//! would produce different bytes on disk.
//!
//! The error policy is the sequential one, unchanged: one bad entry is logged
//! at `warn` and stepped over, the rest of the page still applies, and a later
//! change re-delivers the path.

use futures_util::future::join_all;

use crate::api::RemoteEntry;
use crate::error::Result;
use crate::state::{Direction, StateDb, SyncState};

use super::apply::{self, Plan};
use super::budget::Admission;
use super::paths::PathMapper;
use super::schedule::{self, Step};
use super::source::RemoteSource;

/// The smallest interval between interim state saves inside one page.
///
/// A compromise: the state file is rewritten whole, so saving per entry would
/// make a large pull O(n²) in bytes written, while saving too rarely is what
/// this exists to avoid. At 100 an interrupt costs at most 99 re-downloads.
pub(crate) const CHECKPOINT_EVERY: usize = 100;

/// The largest interval, so the interval below can never grow without bound:
/// a crash may cost at most this many re-applied entries however big the
/// account is. Re-applying is cheap and idempotent; losing a whole page is not.
pub(crate) const CHECKPOINT_AT_MOST: usize = 1000;

/// How many tracked entries buy one entry of extra interval. A save costs
/// bytes proportional to the tracked count, so holding the ratio of
/// bytes-written to entries-applied roughly constant means the interval has to
/// grow with the account rather than staying at a flat 100.
const ENTRIES_PER_TRACKED: usize = 64;

/// How many applied entries between interim saves, given what is tracked.
///
/// A flat interval is what made a large pull quadratic: at 43k tracked files a
/// full-file save every 100 entries is a ~19 MB write 430 times a page, which
/// pegs a core and starves the download loop it is meant to protect.
pub(crate) fn checkpoint_interval(tracked: usize) -> usize {
    (tracked / ENTRIES_PER_TRACKED).clamp(CHECKPOINT_EVERY, CHECKPOINT_AT_MOST)
}

/// Whether `applied` entries into a page is a point to save state at.
pub(crate) fn is_checkpoint(applied: usize, tracked: usize) -> bool {
    applied > 0 && applied.is_multiple_of(checkpoint_interval(tracked))
}

/// Everything applying a page needs, gathered so the borrow of the reconciler
/// is split once, at the top, rather than at every call.
pub(crate) struct Page<'a, S> {
    pub source: &'a S,
    pub paths: &'a PathMapper,
    pub state: &'a mut SyncState,
    pub db: &'a StateDb,
    pub admission: &'a Admission,
}

impl<S: RemoteSource + Sync> Page<'_, S> {
    /// Apply every entry, returning how many succeeded.
    ///
    /// The caller advances the cursor afterwards: it may only move once the
    /// whole page has finished, which is why this returns rather than saving
    /// it. The interim saves it does make never touch the cursor.
    pub async fn apply(&mut self, entries: &[RemoteEntry]) -> Result<usize> {
        let mut applied = 0;
        for step in schedule::partition(entries) {
            match step {
                Step::Serial(entries) => self.apply_serial(&entries, &mut applied).await?,
                Step::Parallel(groups) => self.apply_parallel(&groups, &mut applied).await?,
            }
        }
        Ok(applied)
    }

    /// Apply entries one at a time, in order, with nothing else in flight.
    async fn apply_serial(&mut self, entries: &[&RemoteEntry], applied: &mut usize) -> Result<()> {
        for entry in entries {
            let outcome = apply::apply_entry(self.source, self.paths, self.state, entry).await;
            let pause = rate_limit_of(&outcome);
            self.tally(entry, outcome.map(|_| ()), applied).await?;
            obey(pause).await;
        }
        Ok(())
    }

    /// Apply groups of file entries, overlapping the fetches across groups.
    ///
    /// Groups are walked in *rounds*: the first entry of every group, then the
    /// second, and so on. A group holds the entries for one path, and those
    /// must stay sequential — the second revision of a file has to decide
    /// against the state the first one recorded.
    async fn apply_parallel(
        &mut self,
        groups: &[Vec<&RemoteEntry>],
        applied: &mut usize,
    ) -> Result<()> {
        let depth = groups.iter().map(Vec::len).max().unwrap_or(0);
        for round in 0..depth {
            let entries: Vec<&RemoteEntry> = groups
                .iter()
                .filter_map(|g| g.get(round))
                .copied()
                .collect();
            self.apply_round(&entries, applied).await?;
        }
        Ok(())
    }

    /// One round: decide all, fetch concurrently, then record in listing order.
    async fn apply_round(&mut self, entries: &[&RemoteEntry], applied: &mut usize) -> Result<()> {
        let mut plans: Vec<(&RemoteEntry, Plan<'_>)> = Vec::with_capacity(entries.len());
        for entry in entries {
            match apply::decide(self.paths, self.state, entry).await {
                Ok(plan) => plans.push((entry, plan)),
                // An entry we cannot even plan for never reaches the budget.
                Err(error) => {
                    tracing::warn!(path = entry.display_path(), %error, "could not apply entry")
                }
            }
        }
        let fetched = join_all(plans.iter().map(|(_, plan)| self.fetch(plan))).await;

        let mut pause = None;
        for ((entry, plan), outcome) in plans.iter().zip(fetched) {
            pause = pause.max(rate_limit_of(&outcome));
            let outcome = outcome.and_then(|fetched| apply::record(self.state, plan, fetched));
            self.tally(entry, outcome, applied).await?;
        }
        // Dropbox named a delay; hammering on with the rest of the page would
        // only earn longer ones.
        obey(pause).await;
        Ok(())
    }

    /// Fetch one plan, once the budget admits its bytes.
    async fn fetch(&self, plan: &Plan<'_>) -> Result<apply::Fetched> {
        let permit = self.admission.acquire(plan.size()).await;
        // The permit's cost, not the file's size: an oversized file is clamped
        // to the whole budget, and it may only spend what it actually holds.
        apply::fetch(self.source, plan, permit.cost()).await
    }

    /// Count one outcome, log a failure, and checkpoint on the interval.
    /// Record the local name of an entry whose name had to be shortened.
    ///
    /// Only for names that actually differ: the alias index exists so the
    /// upload direction can find its way back, and an entry whose local name
    /// already matches the remote one needs no help.
    fn remember_any_alias(&mut self, entry: &RemoteEntry) {
        let display_path = entry.display_path();
        let (Ok(local), Ok(plain)) = (
            self.paths.to_local(display_path),
            self.paths.to_local_unshortened(display_path),
        ) else {
            return;
        };
        if local == plain {
            return;
        }
        let Ok(relative) = self.paths.relative_key(&local) else {
            return;
        };
        self.state.record_alias(&relative, display_path);
    }

    async fn tally(
        &mut self,
        entry: &RemoteEntry,
        outcome: Result<()>,
        applied: &mut usize,
    ) -> Result<()> {
        match outcome {
            Ok(()) => {
                *applied += 1;
                // A path that has now arrived is no longer missing. Clearing on
                // every success is what keeps the failure list a list of things
                // that are *currently* wrong rather than a history.
                self.state.clear_failure(entry.display_path());
                self.remember_any_alias(entry);
            }
            // One bad path must not stall the whole stream; the cursor still
            // advances, and a later change re-delivers the path. But it is
            // *recorded*, not merely logged: a warning scrolls away, and a file
            // that failed to download is silently absent from disk otherwise.
            Err(error) => {
                tracing::warn!(path = entry.display_path(), %error, "could not apply entry");
                self.state
                    .record_failure(entry.display_path(), &error, Direction::Download);
                return Ok(());
            }
        }
        if is_checkpoint(*applied, self.state.len()) {
            self.db.save_off_thread(self.state).await?;
            tracing::debug!(
                applied = *applied,
                tracked = self.state.len(),
                "checkpointed mid-page"
            );
        }
        Ok(())
    }
}

/// The wait Dropbox asked for, if this outcome was a 429.
fn rate_limit_of<T>(outcome: &Result<T>) -> Option<u64> {
    match outcome {
        Err(crate::error::Error::RateLimited(seconds)) => Some(*seconds),
        _ => None,
    }
}

/// Honour a named delay before touching the API again.
async fn obey(pause: Option<u64>) {
    if let Some(seconds) = pause {
        tracing::warn!(seconds, "rate limited mid-page; pausing as asked");
        tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 429 on one file's transfer must pause the page for the delay Dropbox
    /// named, instead of hammering straight on with the remaining entries.
    #[tokio::test(start_paused = true)]
    async fn a_rate_limited_download_pauses_the_page() {
        use crate::api::{Allowance, RemoteEntry, RemoteFile};
        use crate::error::Error;
        use crate::state::{StateDb, SyncState};
        use std::path::Path;

        struct RateLimitedOnce;
        impl crate::reconcile::source::RemoteSource for RateLimitedOnce {
            async fn list_folder(&self, _: &str) -> Result<crate::api::ListFolderPage> {
                unreachable!()
            }
            async fn list_folder_continue(&self, _: &str) -> Result<crate::api::ListFolderPage> {
                unreachable!()
            }
            async fn get_metadata(&self, _: &str) -> Result<RemoteEntry> {
                unreachable!()
            }
            async fn download_to(
                &self,
                remote_path: &str,
                _rev: &str,
                _allowance: Allowance,
                _expected_hash: Option<&str>,
                dest: &Path,
            ) -> Result<()> {
                match remote_path {
                    "/limited.txt" => Err(Error::RateLimited(7)),
                    _ => {
                        tokio::fs::write(dest, b"ok").await?;
                        Ok(())
                    }
                }
            }
        }

        fn file(path: &str) -> RemoteEntry {
            RemoteEntry::File(RemoteFile {
                path_lower: path.to_lowercase(),
                path_display: path.to_string(),
                rev: "r1".to_string(),
                size: 0,
                content_hash: None,
            })
        }

        let dir = tempfile::tempdir().unwrap();
        let mut state = SyncState::new();
        let db = StateDb::at(dir.path().join("state.json"));
        let admission = crate::reconcile::Admission::default();
        let mut page = Page {
            source: &RateLimitedOnce,
            paths: &PathMapper::new(dir.path(), ""),
            state: &mut state,
            db: &db,
            admission: &admission,
        };

        let started = tokio::time::Instant::now();
        let applied = page
            .apply(&[file("/limited.txt"), file("/fine.txt")])
            .await
            .unwrap();

        assert_eq!(applied, 1, "the healthy entry still lands");
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(7),
            "the named delay must be honoured"
        );
    }

    /// Interim saves land on the interval and nowhere else. The end-to-end
    /// case — killing the process mid-page — cannot be reached from a unit
    /// test, so the interval itself is what gets covered here.
    ///
    /// The count is of *completed* entries, which under concurrency is no
    /// longer a prefix of the page: entry 140 can be done while 100 is still in
    /// flight. That is safe because an interim save only ever adds entries that
    /// really are on disk and never touches the cursor — the cursor moves at
    /// the page barrier, in `Reconciler::apply_page`.
    #[test]
    fn state_is_checkpointed_on_the_interval_only() {
        assert!(!is_checkpoint(0, 0), "no save before anything is applied");
        assert!(!is_checkpoint(CHECKPOINT_EVERY - 1, 0));
        assert!(is_checkpoint(CHECKPOINT_EVERY, 0));
        assert!(!is_checkpoint(CHECKPOINT_EVERY + 1, 0));
        assert!(is_checkpoint(CHECKPOINT_EVERY * 3, 0));
    }

    /// A small account keeps the floor: the interval exists to bound re-work,
    /// and re-work is what matters when a save is cheap.
    #[test]
    fn a_small_account_checkpoints_on_the_floor() {
        assert_eq!(checkpoint_interval(0), CHECKPOINT_EVERY);
        assert_eq!(checkpoint_interval(100), CHECKPOINT_EVERY);
        assert_eq!(
            checkpoint_interval(CHECKPOINT_EVERY * ENTRIES_PER_TRACKED),
            CHECKPOINT_EVERY,
            "the floor holds right up to where the ratio takes over"
        );
    }

    /// The interval grows with the account, which is the whole point: a save
    /// costs bytes proportional to what is tracked.
    #[test]
    fn a_large_account_checkpoints_less_often() {
        assert_eq!(checkpoint_interval(43_000), 43_000 / ENTRIES_PER_TRACKED);
        assert!(
            checkpoint_interval(43_000) > checkpoint_interval(8_500),
            "a bigger account must not save as often as a smaller one"
        );
    }

    /// However large the account, a crash may not cost an unbounded page.
    #[test]
    fn the_interval_is_capped_however_big_the_account() {
        assert_eq!(checkpoint_interval(usize::MAX), CHECKPOINT_AT_MOST);
        assert_eq!(checkpoint_interval(10_000_000), CHECKPOINT_AT_MOST);
    }
}
