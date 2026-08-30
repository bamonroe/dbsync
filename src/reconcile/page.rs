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
use crate::state::{StateDb, SyncState};

use super::apply::{self, Plan};
use super::budget::Admission;
use super::paths::PathMapper;
use super::schedule::{self, Step};
use super::source::RemoteSource;

/// How many applied entries between interim state saves inside one page.
///
/// A compromise: the state file is rewritten whole, so saving per entry would
/// make a large pull O(n²) in bytes written, while saving too rarely is what
/// this exists to avoid. At 100 an interrupt costs at most 99 re-downloads.
pub(crate) const CHECKPOINT_EVERY: usize = 100;

/// Whether `applied` entries into a page is a point to save state at.
pub(crate) fn is_checkpoint(applied: usize) -> bool {
    applied > 0 && applied.is_multiple_of(CHECKPOINT_EVERY)
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
            self.tally(entry, outcome.map(|_| ()), applied)?;
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
            match apply::decide(self.paths, self.state, entry) {
                Ok(plan) => plans.push((entry, plan)),
                // An entry we cannot even plan for never reaches the budget.
                Err(error) => {
                    tracing::warn!(path = entry.display_path(), %error, "could not apply entry")
                }
            }
        }
        let fetched = join_all(plans.iter().map(|(_, plan)| self.fetch(plan))).await;

        for ((entry, plan), outcome) in plans.iter().zip(fetched) {
            let outcome = outcome.and_then(|()| apply::record(self.state, plan));
            self.tally(entry, outcome, applied)?;
        }
        Ok(())
    }

    /// Fetch one plan, once the budget admits its bytes.
    async fn fetch(&self, plan: &Plan<'_>) -> Result<()> {
        let permit = self.admission.acquire(plan.size()).await;
        // The permit's cost, not the file's size: an oversized file is clamped
        // to the whole budget, and it may only spend what it actually holds.
        apply::fetch(self.source, plan, permit.cost())
            .await
            .map(|_| ())
    }

    /// Count one outcome, log a failure, and checkpoint on the interval.
    fn tally(
        &mut self,
        entry: &RemoteEntry,
        outcome: Result<()>,
        applied: &mut usize,
    ) -> Result<()> {
        match outcome {
            Ok(()) => *applied += 1,
            // One bad path must not stall the whole stream; the cursor still
            // advances, and a later change re-delivers the path.
            Err(error) => {
                tracing::warn!(path = entry.display_path(), %error, "could not apply entry");
                return Ok(());
            }
        }
        if is_checkpoint(*applied) {
            self.db.save(self.state)?;
            tracing::debug!(applied = *applied, "checkpointed mid-page");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!is_checkpoint(0), "no save before anything is applied");
        assert!(!is_checkpoint(CHECKPOINT_EVERY - 1));
        assert!(is_checkpoint(CHECKPOINT_EVERY));
        assert!(!is_checkpoint(CHECKPOINT_EVERY + 1));
        assert!(is_checkpoint(CHECKPOINT_EVERY * 3));
    }
}
