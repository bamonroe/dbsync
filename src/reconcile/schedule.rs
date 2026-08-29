//! Splitting a page of entries into what may run concurrently and what may not.
//!
//! Downloads are independent of each other; deletes are not. [`apply_delete`]
//! forgets a whole subtree and removes the directory, so a tombstone overlapping
//! a download into that subtree could delete a file that has just landed — and
//! a folder tombstone is the *only* notice that its children are gone, so there
//! is no per-child event to order against. A tombstone is therefore a
//! **barrier**: everything already in flight drains before it is applied.
//!
//! Within the concurrent part, entries for one path stay sequential. A page can
//! hold more than one entry for a path (two revisions, or a create then a
//! delete), and those must not race each other. Paths are grouped by
//! [`key_for`], the same case-folding identity the state uses, because Dropbox
//! is case-insensitive and `/A.txt` and `/a.txt` are one file.
//!
//! Folder entries are serial work too — a `mkdir` is cheap and not worth a task
//! — but they are deliberately *not* barriers. They can be applied after files
//! listed before them because [`download_to`](crate::api::download) creates the
//! parent directories it needs, so a file arriving before its folder entry is
//! already safe, and making every folder a barrier would shred the parallelism
//! of a first listing, where folders are interleaved throughout. A folder may
//! therefore be applied a little out of listing order relative to files, which
//! is harmless: `mkdir` is idempotent and touches nothing a download touches.
//!
//! This is a pure function over a page: no remote, no disk, no state.
//!
//! [`apply_delete`]: super::apply
//! [`key_for`]: crate::state::key_for

use std::collections::HashMap;

use crate::api::RemoteEntry;
use crate::state::key_for;

/// One unit of work from a page, in the order the units must be applied.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<'a> {
    /// Entries to apply one at a time, in this order, with nothing else in
    /// flight: tombstones and folders.
    Serial(Vec<&'a RemoteEntry>),
    /// Groups of file entries. The groups may run concurrently; the entries
    /// *within* a group are for one path and must run in order.
    Parallel(Vec<Vec<&'a RemoteEntry>>),
}

impl Step<'_> {
    /// How many entries this step covers, whatever its shape.
    pub fn len(&self) -> usize {
        match self {
            Self::Serial(entries) => entries.len(),
            Self::Parallel(groups) => groups.iter().map(Vec::len).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Whether an entry may be applied concurrently with other paths.
fn is_parallel(entry: &RemoteEntry) -> bool {
    matches!(entry, RemoteEntry::File(_))
}

/// Whether an entry forces everything in flight to finish first.
fn is_barrier(entry: &RemoteEntry) -> bool {
    matches!(entry, RemoteEntry::Deleted(_))
}

/// Accumulates entries until a barrier or the end of the page flushes them.
#[derive(Default)]
struct Batch<'a> {
    /// Per-path groups, in the order their first entry was seen.
    groups: Vec<Vec<&'a RemoteEntry>>,
    /// Where a path's group lives in `groups`.
    index: HashMap<String, usize>,
    /// Folders and tombstones awaiting their turn, in listing order.
    serial: Vec<&'a RemoteEntry>,
}

impl<'a> Batch<'a> {
    fn push_parallel(&mut self, entry: &'a RemoteEntry) {
        let key = key_for(entry.display_path());
        match self.index.get(&key) {
            Some(&at) => self.groups[at].push(entry),
            None => {
                self.index.insert(key, self.groups.len());
                self.groups.push(vec![entry]);
            }
        }
    }

    /// Emit the concurrent work accumulated so far.
    fn flush_parallel(&mut self, into: &mut Vec<Step<'a>>) {
        if !self.groups.is_empty() {
            into.push(Step::Parallel(std::mem::take(&mut self.groups)));
            self.index.clear();
        }
    }

    /// Emit the serial work accumulated so far.
    fn flush_serial(&mut self, into: &mut Vec<Step<'a>>) {
        if !self.serial.is_empty() {
            into.push(Step::Serial(std::mem::take(&mut self.serial)));
        }
    }
}

/// Split a page into steps that preserve every ordering constraint while
/// letting independent downloads overlap.
///
/// The returned steps are applied in order; only a [`Step::Parallel`]'s groups
/// run at the same time as one another.
pub fn partition(entries: &[RemoteEntry]) -> Vec<Step<'_>> {
    let mut steps = Vec::new();
    let mut batch = Batch::default();
    for entry in entries {
        if is_parallel(entry) {
            // Serial work already queued has to land before new downloads
            // start, or a tombstone would end up running after files listed
            // after it.
            batch.flush_serial(&mut steps);
            batch.push_parallel(entry);
        } else {
            // A tombstone drains what is in flight before it lands; a folder
            // just queues, and is applied whenever its turn comes.
            if is_barrier(entry) {
                batch.flush_parallel(&mut steps);
            }
            batch.serial.push(entry);
        }
    }
    batch.flush_parallel(&mut steps);
    batch.flush_serial(&mut steps);
    steps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{RemoteDeleted, RemoteFile, RemoteFolder};

    fn file(path: &str, rev: &str) -> RemoteEntry {
        RemoteEntry::File(RemoteFile {
            path_lower: path.to_lowercase(),
            path_display: path.to_string(),
            rev: rev.to_string(),
            size: 0,
            content_hash: None,
        })
    }

    fn folder(path: &str) -> RemoteEntry {
        RemoteEntry::Folder(RemoteFolder {
            path_lower: path.to_lowercase(),
            path_display: path.to_string(),
        })
    }

    fn tombstone(path: &str) -> RemoteEntry {
        RemoteEntry::Deleted(RemoteDeleted {
            path_lower: path.to_lowercase(),
            path_display: Some(path.to_string()),
        })
    }

    /// The paths a step covers, flattened, for terse assertions.
    fn paths(step: &Step<'_>) -> Vec<Vec<String>> {
        match step {
            Step::Serial(entries) => entries
                .iter()
                .map(|e| vec![e.display_path().to_string()])
                .collect(),
            Step::Parallel(groups) => groups
                .iter()
                .map(|g| g.iter().map(|e| e.display_path().to_string()).collect())
                .collect(),
        }
    }

    /// The common case — a first listing is all files, and all of it overlaps.
    #[test]
    fn a_page_of_distinct_files_is_one_parallel_step() {
        let entries = vec![file("/a", "r1"), file("/b", "r1"), file("/c", "r1")];
        let steps = partition(&entries);
        assert_eq!(steps.len(), 1);
        assert_eq!(paths(&steps[0]), [["/a"], ["/b"], ["/c"]]);
    }

    /// Two entries for one path must not race: same group, listing order.
    #[test]
    fn two_revisions_of_one_path_stay_sequential() {
        let entries = vec![file("/a", "r1"), file("/b", "r1"), file("/a", "r2")];
        let steps = partition(&entries);
        assert_eq!(steps.len(), 1);
        assert_eq!(paths(&steps[0]), [vec!["/a", "/a"], vec!["/b"]]);
    }

    /// Dropbox is case-insensitive, so `/A` and `/a` are one path and must land
    /// in one group — grouping on the raw display path would let them race.
    #[test]
    fn paths_are_grouped_case_insensitively() {
        let entries = vec![file("/A.txt", "r1"), file("/a.txt", "r2")];
        let steps = partition(&entries);
        assert_eq!(paths(&steps[0]), [vec!["/A.txt", "/a.txt"]]);
    }

    /// The load-bearing rule: a tombstone drains the downloads listed before it
    /// rather than running alongside them.
    #[test]
    fn a_tombstone_is_a_barrier() {
        let entries = vec![
            file("/a", "r1"),
            tombstone("/dir"),
            file("/b", "r1"),
            file("/c", "r1"),
        ];
        let steps = partition(&entries);
        assert_eq!(paths(&steps[0]), [["/a"]]);
        assert_eq!(paths(&steps[1]), [["/dir"]]);
        assert_eq!(paths(&steps[2]), [["/b"], ["/c"]]);
        assert!(matches!(steps[1], Step::Serial(_)));
    }

    /// Consecutive tombstones need only one drain between them, and stay in
    /// listing order within the serial step.
    #[test]
    fn consecutive_tombstones_share_one_barrier() {
        let entries = vec![
            file("/a", "r1"),
            tombstone("/x"),
            tombstone("/y"),
            file("/b", "r1"),
        ];
        let steps = partition(&entries);
        assert_eq!(steps.len(), 3);
        assert_eq!(paths(&steps[1]), [["/x"], ["/y"]]);
    }

    /// A create then a delete of one path: the barrier keeps them ordered, so
    /// the file is not left on disk by a download that finished after it.
    #[test]
    fn a_file_and_its_tombstone_stay_ordered() {
        let entries = vec![file("/a", "r1"), tombstone("/a")];
        let steps = partition(&entries);
        assert_eq!(paths(&steps[0]), [["/a"]]);
        assert!(matches!(steps[1], Step::Serial(_)));
    }

    /// A folder is serial work but not a barrier: the files around it still
    /// share one parallel step, because a download creates its own parents.
    #[test]
    fn a_folder_does_not_split_the_parallel_work() {
        let entries = vec![file("/a", "r1"), folder("/dir"), file("/dir/b", "r1")];
        let steps = partition(&entries);
        assert_eq!(steps.len(), 2);
        assert_eq!(paths(&steps[0]), [["/dir"]]);
        assert_eq!(paths(&steps[1]), [["/a"], ["/dir/b"]]);
    }

    /// Serial entries keep their relative order across a barrier flush.
    #[test]
    fn folders_and_tombstones_keep_their_relative_order() {
        let entries = vec![folder("/dir"), tombstone("/gone"), folder("/other")];
        let steps = partition(&entries);
        assert_eq!(steps.len(), 1);
        assert_eq!(paths(&steps[0]), [["/dir"], ["/gone"], ["/other"]]);
    }

    /// Every entry is scheduled exactly once — a partition that drops work
    /// would silently skip files.
    #[test]
    fn every_entry_is_scheduled_exactly_once() {
        let entries = vec![
            file("/a", "r1"),
            folder("/dir"),
            tombstone("/x"),
            file("/a", "r2"),
            file("/b", "r1"),
        ];
        let scheduled: usize = partition(&entries).iter().map(Step::len).sum();
        assert_eq!(scheduled, entries.len());
    }

    #[test]
    fn an_empty_page_schedules_nothing() {
        assert!(partition(&[]).is_empty());
    }
}
