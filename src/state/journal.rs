//! The append-only journal that makes saving the state cheap.
//!
//! [`super::StateDb::save`] used to rewrite `state.json` whole. That is fine for
//! a few hundred files and quadratic for a large account: a pull checkpoints as
//! it goes, and each checkpoint rewrites every entry the state holds, so the
//! bytes written grow with (entries × checkpoints). On a live account this was
//! measured pinning the CPU at ~50% while the file count sat still — a 4 MB
//! rewrite roughly once a second, on the way to 19 MB a time.
//!
//! So a save now appends only what *changed* since the last one. The snapshot
//! stays the base and the journal carries the deltas on top of it; loading
//! reads the snapshot and replays the journal. The snapshot is rewritten only
//! when the journal has grown long enough to be worth folding in
//! ([`COMPACT_AFTER`]), which turns an O(n) write per checkpoint into an
//! O(changes) one with an occasional O(n).
//!
//! **A torn tail is expected, not exceptional.** A crash mid-append leaves a
//! half-written last line, so replay stops at the first record it cannot parse
//! rather than refusing to load. The cost is losing the last few changes, which
//! the next pull re-delivers; the alternative — a state file that will not open
//! — is far worse.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use super::entry::SyncEntry;
use super::failures::Failure;
use crate::error::{Error, Result};

/// How many journal records justify folding them back into the snapshot.
///
/// The trade is bounded either way: too low and large accounts pay the whole-
/// file write often, too high and startup replays a long journal. A few
/// thousand records is a small replay and a rare rewrite.
pub const COMPACT_AFTER: usize = 5_000;

/// One change to the state, as written to the journal.
///
/// Deliberately one variant per mutation the state exposes, rather than a
/// generic patch: replaying must reproduce exactly what the mutating method did,
/// and a shape that can express more than the API can is a shape that can drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Record {
    /// An entry was added or replaced.
    Entry(Box<SyncEntry>),
    /// An entry was forgotten, by key.
    EntryGone(String),
    /// A failure was recorded or updated, by key.
    Failure(String, Box<Failure>),
    /// A failure was cleared, by key.
    FailureGone(String),
    /// A shortened local name was mapped to its remote path.
    Alias(String, String),
    /// A folder's true casing, by lowercased relative path.
    FolderCase(String, String),
    /// The cursor moved, or was dropped.
    Cursor(Option<String>),
}

/// Stands in for "the cached count is not known yet" — a real journal can
/// never hold this many records.
const UNKNOWN: usize = usize::MAX;

/// The journal file sitting beside a snapshot.
///
/// Cloning shares the cached record count, so the clone a save hands to a
/// blocking thread keeps the counting work the original already did.
#[derive(Debug, Clone)]
pub struct Journal {
    path: PathBuf,
    /// How many records the file holds, or [`UNKNOWN`] before anything has
    /// looked. Every write goes through this type, so the count can be kept up
    /// to date instead of recounted: [`Self::record_count`] runs on every save,
    /// and re-parsing the whole journal there made saving O(journal length) —
    /// quadratic across a long pull, which is the cost this module exists to
    /// remove.
    count: Arc<AtomicUsize>,
}

impl Journal {
    /// The journal belonging to the snapshot at `snapshot_path`.
    pub fn beside(snapshot_path: &Path) -> Self {
        Self {
            path: snapshot_path.with_extension("journal"),
            count: Arc::new(AtomicUsize::new(UNKNOWN)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append `records`, then flush and sync so they survive a power loss.
    ///
    /// One open and one sync for the whole batch: syncing per record would put
    /// the cost straight back that this module exists to remove.
    pub fn append(&self, records: &[Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut writer = std::io::BufWriter::new(file);
        for record in records {
            let line = serde_json::to_string(record)
                .map_err(|e| Error::Config(format!("cannot serialise a journal record: {e}")))?;
            writeln!(writer, "{line}")?;
        }
        writer.flush()?;
        writer
            .into_inner()
            .map_err(|e| Error::Config(format!("cannot flush the journal: {e}")))?
            .sync_all()?;
        self.count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |known| {
                (known != UNKNOWN).then(|| known + records.len())
            })
            .ok();
        Ok(())
    }

    /// Every record that survived, in order, stopping at the first torn one.
    pub fn replay(&self) -> Result<Vec<Record>> {
        let Some(file) = crate::fsutil::open_optional(&self.path)? else {
            self.count.store(0, Ordering::Relaxed);
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        for line in BufReader::new(file).lines() {
            // An unreadable line is a torn tail, and everything after it is
            // unordered relative to it, so replay stops rather than skipping.
            let Ok(line) = line else { break };
            match serde_json::from_str(&line) {
                Ok(record) => records.push(record),
                Err(_) => break,
            }
        }
        self.count.store(records.len(), Ordering::Relaxed);
        Ok(records)
    }

    /// How many records the journal currently holds.
    ///
    /// Reads the file only the first time; after that the count is kept current
    /// by the writes themselves. A load replays the journal on the way in, so in
    /// practice the daemon has already paid for it before the first save.
    pub fn record_count(&self) -> Result<usize> {
        match self.count.load(Ordering::Relaxed) {
            UNKNOWN => Ok(self.replay()?.len()),
            known => Ok(known),
        }
    }

    /// Drop the journal, which a fresh snapshot has just made redundant.
    pub fn clear(&self) -> Result<()> {
        crate::fsutil::remove_if_present(&self.path)?;
        self.count.store(0, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal() -> (tempfile::TempDir, Journal) {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::beside(&dir.path().join("state.json"));
        (dir, journal)
    }

    #[test]
    fn records_replay_in_the_order_they_were_appended() {
        let (_dir, journal) = journal();
        journal
            .append(&[
                Record::Cursor(Some("c1".into())),
                Record::EntryGone("/a.txt".into()),
            ])
            .unwrap();
        journal
            .append(&[Record::Cursor(Some("c2".into()))])
            .unwrap();

        assert_eq!(
            journal.replay().unwrap(),
            vec![
                Record::Cursor(Some("c1".into())),
                Record::EntryGone("/a.txt".into()),
                Record::Cursor(Some("c2".into())),
            ]
        );
    }

    /// A crash mid-append leaves a half-written line. Losing it costs the last
    /// few changes, which the next pull re-delivers; refusing to load would
    /// cost the whole state.
    #[test]
    fn a_torn_last_line_stops_the_replay_rather_than_failing_it() {
        let (_dir, journal) = journal();
        journal
            .append(&[Record::Cursor(Some("c1".into()))])
            .unwrap();
        let mut text = std::fs::read_to_string(journal.path()).unwrap();
        text.push_str("{\"Cursor\":[\"half");
        std::fs::write(journal.path(), text).unwrap();

        assert_eq!(
            journal.replay().unwrap(),
            vec![Record::Cursor(Some("c1".into()))]
        );
    }

    /// Everything after a torn record is unordered relative to it, so replay
    /// must stop, not skip.
    #[test]
    fn replay_stops_at_a_tear_instead_of_skipping_past_it() {
        let (_dir, journal) = journal();
        journal
            .append(&[Record::Cursor(Some("c1".into()))])
            .unwrap();
        let mut text = std::fs::read_to_string(journal.path()).unwrap();
        text.push_str("garbage\n");
        std::fs::write(journal.path(), text).unwrap();
        journal
            .append(&[Record::Cursor(Some("c3".into()))])
            .unwrap();

        assert_eq!(journal.replay().unwrap().len(), 1);
    }

    #[test]
    fn an_absent_journal_replays_as_nothing() {
        let (_dir, journal) = journal();
        assert!(journal.replay().unwrap().is_empty());
        assert_eq!(journal.record_count().unwrap(), 0);
    }

    #[test]
    fn clearing_removes_the_file() {
        let (_dir, journal) = journal();
        journal.append(&[Record::Cursor(None)]).unwrap();
        journal.clear().unwrap();

        assert!(!journal.path().exists());
        assert!(journal.replay().unwrap().is_empty());
    }

    /// The count a save consults must track appends and clears without going
    /// back to the file, since re-reading it on every save is what made saving
    /// cost the whole journal.
    #[test]
    fn the_record_count_follows_appends_and_clears_without_re_reading() {
        let (_dir, journal) = journal();
        assert_eq!(journal.record_count().unwrap(), 0);

        journal
            .append(&[Record::Cursor(None), Record::EntryGone("/a.txt".into())])
            .unwrap();
        journal.append(&[Record::Cursor(None)]).unwrap();
        assert_eq!(journal.record_count().unwrap(), 3);

        // With the file gone, only a cached count can still answer 3.
        std::fs::remove_file(journal.path()).unwrap();
        assert_eq!(journal.record_count().unwrap(), 3);

        journal.clear().unwrap();
        assert_eq!(journal.record_count().unwrap(), 0);
    }

    /// A save moves the db — and so the journal — to a blocking thread and back,
    /// which must not throw the count away and start recounting.
    #[test]
    fn a_clone_shares_the_cached_count() {
        let (_dir, journal) = journal();
        assert_eq!(journal.record_count().unwrap(), 0);
        let clone = journal.clone();
        clone.append(&[Record::Cursor(None)]).unwrap();

        std::fs::remove_file(journal.path()).unwrap();
        assert_eq!(journal.record_count().unwrap(), 1);
    }

    /// Nothing has looked at the file yet, so the first question has to read it.
    #[test]
    fn a_journal_left_by_a_previous_run_is_counted_on_first_ask() {
        let (dir, journal) = journal();
        journal
            .append(&[Record::Cursor(None), Record::Cursor(None)])
            .unwrap();

        let reopened = Journal::beside(&dir.path().join("state.json"));
        assert_eq!(reopened.record_count().unwrap(), 2);
    }
}
