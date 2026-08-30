//! The sync state database and its atomic persistence.
//!
//! The whole state is one JSON document rewritten as a unit. That is the right
//! trade at this size: the file is small, and a single atomic replace is much
//! easier to reason about than incremental updates that could half-apply. The
//! invariant that matters is in `docs/architecture.md` — a crash must never
//! leave the state and the disk disagreeing.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::entry::SyncEntry;
use super::failures::{self, Direction, Failure};
use super::journal::{COMPACT_AFTER, Journal, Record};
use crate::error::{Error, Result};

/// Bumped when the on-disk shape changes incompatibly. A state file from a
/// future version is refused rather than silently misread.
const STATE_VERSION: u32 = 1;

/// Everything dbsync knows about the last agreed sync.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncState {
    #[serde(default = "default_version")]
    version: u32,

    /// The Dropbox folder cursor: our position in the remote change stream.
    ///
    /// Losing it costs a full re-list, not data. See `docs/architecture.md`.
    #[serde(default)]
    cursor: Option<String>,

    /// Entries keyed by **lowercased** path.
    ///
    /// Dropbox treats paths case-insensitively, so `/Photos/Cat.jpg` and
    /// `/photos/cat.jpg` are the same file. Keying on the lowercase form is
    /// what stops a case change from being read as a second file; the original
    /// casing is preserved in [`SyncEntry::display_path`].
    #[serde(default)]
    entries: BTreeMap<String, SyncEntry>,

    /// Entries that could not be applied, keyed the same way as `entries`.
    ///
    /// Kept in the state file rather than only in the log so that "what is
    /// missing locally?" is a question with an answer after a restart. See
    /// [`super::failures`].
    #[serde(default)]
    failures: BTreeMap<String, Failure>,

    /// Remote paths whose local name had to be shortened, keyed by the
    /// lowercased local path relative to the sync root.
    ///
    /// Without this the local-to-remote direction would be lost: the on-disk
    /// name no longer says what the remote one was, so an edit to a shortened
    /// file would be uploaded as a *new* remote file under the shortened name.
    /// Only names that actually needed shortening appear here, so it is empty
    /// for almost every account.
    #[serde(default)]
    aliases: BTreeMap<String, String>,

    /// The true casing of every folder, keyed by its lowercased path relative to
    /// the sync root.
    ///
    /// Dropbox only capitalises the *last* component of a `path_display`: ask it
    /// for a file and the folders above it can come back lowercased, while the
    /// folder's own entry names it correctly. Creating a deep file's parents
    /// straight from its display path therefore bakes in the wrong case, which
    /// a case-sensitive filesystem then keeps forever. Folder entries are
    /// recorded here as they arrive and every later path is rebuilt through
    /// them. See [`crate::reconcile::dircase`].
    #[serde(default)]
    folders: BTreeMap<String, String>,

    /// What has changed since the last save, so a save can write the deltas
    /// instead of the whole file. Not serialised: it describes the difference
    /// between this state and what is on disk, which is meaningless once it
    /// *is* what is on disk.
    #[serde(skip)]
    pending: Vec<Record>,
}

fn default_version() -> u32 {
    STATE_VERSION
}

/// The lookup key for a Dropbox path.
pub fn key_for(path: &str) -> String {
    path.to_lowercase()
}

impl SyncState {
    /// An empty state with no cursor and no entries.
    pub fn new() -> Self {
        Self {
            version: STATE_VERSION,
            cursor: None,
            entries: BTreeMap::new(),
            failures: BTreeMap::new(),
            aliases: BTreeMap::new(),
            folders: BTreeMap::new(),
            pending: Vec::new(),
        }
    }

    /// Our position in the remote change stream, if we have one.
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Advance the cursor after applying a batch of changes.
    pub fn set_cursor(&mut self, cursor: impl Into<String>) {
        self.cursor = Some(cursor.into());
        self.pending.push(Record::Cursor(self.cursor.clone()));
    }

    /// Forget the cursor. Called when Dropbox resets it, which forces a
    /// re-list; the entries stay, so the re-list reconciles rather than
    /// re-downloads.
    pub fn clear_cursor(&mut self) {
        self.cursor = None;
        self.pending.push(Record::Cursor(None));
    }

    /// The entry for a path, matched case-insensitively.
    pub fn get(&self, path: &str) -> Option<&SyncEntry> {
        self.entries.get(&key_for(path))
    }

    /// Record or replace the entry for a path.
    pub fn insert(&mut self, entry: SyncEntry) {
        self.pending.push(Record::Entry(Box::new(entry.clone())));
        self.entries.insert(key_for(&entry.display_path), entry);
    }

    /// Forget a path, returning what was there.
    pub fn remove(&mut self, path: &str) -> Option<SyncEntry> {
        let key = key_for(path);
        let removed = self.entries.remove(&key);
        if removed.is_some() {
            self.pending.push(Record::EntryGone(key));
        }
        removed
    }

    /// Every known entry, in stable key order.
    pub fn entries(&self) -> impl Iterator<Item = &SyncEntry> {
        self.entries.values()
    }

    /// How many files are tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remember that `path` could not be applied, folding into any existing
    /// record so the attempt count and first sighting survive.
    pub fn record_failure(&mut self, path: &str, error: &Error, direction: Direction) {
        let kind = failures::classify(error);
        let text = error.to_string();
        let key = key_for(path);
        let failure = self
            .failures
            .entry(key.clone())
            .and_modify(|failure| failure.record_again(text.clone(), kind, direction))
            .or_insert_with(|| Failure::new(path, text, kind, direction));
        self.pending
            .push(Record::Failure(key, Box::new(failure.clone())));
    }

    /// Remember that the file at local path `relative` is really `display_path`
    /// remotely, because the name had to be shortened to fit on disk.
    pub fn record_alias(&mut self, relative: &str, display_path: &str) {
        self.pending.push(Record::Alias(
            relative.to_lowercase(),
            display_path.to_string(),
        ));
        self.aliases
            .insert(relative.to_lowercase(), display_path.to_string());
    }

    /// Remember a folder's true casing. `canonical` is its path relative to the
    /// sync root, already rebuilt through the folders above it.
    ///
    /// Recording a folder whose case has not changed is a no-op, so a re-list
    /// does not fill the journal with entries that say nothing.
    pub fn record_folder_case(&mut self, canonical: &str) {
        let key = canonical.to_lowercase();
        if self.folders.get(&key).is_some_and(|held| held == canonical) {
            return;
        }
        self.pending
            .push(Record::FolderCase(key.clone(), canonical.to_string()));
        self.folders.insert(key, canonical.to_string());
    }

    /// The true casing of a folder, given its lowercased relative path.
    pub fn folder_case(&self, lowercased: &str) -> Option<&str> {
        self.folders.get(lowercased).map(String::as_str)
    }

    /// How many folders have a recorded casing.
    pub fn folder_case_count(&self) -> usize {
        self.folders.len()
    }

    /// The remote path of a shortened local file, if this is one.
    pub fn alias_for(&self, relative: &str) -> Option<&str> {
        self.aliases
            .get(&relative.to_lowercase())
            .map(String::as_str)
    }

    /// How many local names had to be shortened.
    pub fn alias_count(&self) -> usize {
        self.aliases.len()
    }

    /// Forget any failure for `path`. Called on every success, so a file that
    /// later arrives stops being reported as missing.
    pub fn clear_failure(&mut self, path: &str) -> Option<Failure> {
        let key = key_for(path);
        let removed = self.failures.remove(&key);
        if removed.is_some() {
            self.pending.push(Record::FailureGone(key));
        }
        removed
    }

    /// Record a prepared failure verbatim. For tests and for migrating a
    /// record between keys; ordinary recording goes through
    /// [`Self::record_failure`], which folds attempts and timestamps.
    pub fn insert_failure(&mut self, path: &str, failure: Failure) {
        let key = key_for(path);
        self.pending
            .push(Record::Failure(key.clone(), Box::new(failure.clone())));
        self.failures.insert(key, failure);
    }

    /// Every recorded failure, in stable key order.
    pub fn failures(&self) -> impl Iterator<Item = &Failure> {
        self.failures.values()
    }

    /// Whether `path` is currently recorded as failed.
    pub fn is_failed(&self, path: &str) -> bool {
        self.failures.contains_key(&key_for(path))
    }

    /// How many entries are recorded as failed.
    pub fn failure_count(&self) -> usize {
        self.failures.len()
    }

    /// The paths worth trying again in `direction`, newest classification
    /// respected.
    ///
    /// Filtered by direction because the two retry passes are not
    /// interchangeable: re-fetching a path whose *upload* failed would pull the
    /// remote copy over the local edit that never got sent.
    pub fn retryable_failures(&self, direction: Direction) -> impl Iterator<Item = &Failure> {
        self.failures
            .values()
            .filter(move |f| f.kind.is_retryable() && f.direction == direction)
    }

    /// Apply one journal record, as replay does on load.
    ///
    /// Goes through the fields directly rather than the mutating methods, which
    /// would queue the record all over again as pending.
    fn replay(&mut self, record: Record) {
        match record {
            Record::Entry(entry) => {
                self.entries.insert(key_for(&entry.display_path), *entry);
            }
            Record::EntryGone(key) => {
                self.entries.remove(&key);
            }
            Record::Failure(key, failure) => {
                self.failures.insert(key, *failure);
            }
            Record::FailureGone(key) => {
                self.failures.remove(&key);
            }
            Record::Alias(relative, display_path) => {
                self.aliases.insert(relative, display_path);
            }
            Record::FolderCase(key, canonical) => {
                self.folders.insert(key, canonical);
            }
            Record::Cursor(cursor) => self.cursor = cursor,
        }
    }
}

/// Loads and atomically saves a [`SyncState`] at a fixed path.
pub struct StateDb {
    path: PathBuf,
}

impl StateDb {
    pub fn at(path: PathBuf) -> Self {
        Self { path }
    }

    /// The default location: `$XDG_DATA_HOME/dbsync/state.json`, alongside the
    /// credentials.
    pub fn default_location() -> Result<Self> {
        let dirs = directories::ProjectDirs::from("", "", "dbsync")
            .ok_or_else(|| Error::Config("cannot determine a home directory".into()))?;
        Ok(Self::at(dirs.data_dir().join("state.json")))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The journal carrying whatever the snapshot does not yet include.
    pub fn journal(&self) -> Journal {
        Journal::beside(&self.path)
    }

    /// Load the snapshot and replay the journal on top of it.
    pub fn load(&self) -> Result<SyncState> {
        let mut state = self.load_snapshot()?;
        let records = self.journal().replay()?;
        if !records.is_empty() {
            tracing::debug!(records = records.len(), "replaying the state journal");
        }
        for record in records {
            state.replay(record);
        }
        // Everything just replayed is already on disk, in the journal.
        state.pending.clear();
        Ok(state)
    }

    /// Load the snapshot alone, ignoring the journal.
    fn load_snapshot(&self) -> Result<SyncState> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SyncState::new()),
            Err(source) => {
                return Err(Error::ReadFile {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        let state: SyncState = serde_json::from_str(&text)
            .map_err(|e| Error::Config(format!("{}: {e}", self.path.display())))?;
        if state.version > STATE_VERSION {
            return Err(Error::Config(format!(
                "{} was written by a newer dbsync (state version {}, this build understands {})",
                self.path.display(),
                state.version,
                STATE_VERSION,
            )));
        }
        Ok(state)
    }

    /// Persist whatever has changed since the last save.
    ///
    /// The cheap path appends the pending records to the journal, which costs
    /// what actually changed rather than the size of the whole account — the
    /// difference between an O(n) write per checkpoint and an O(changes) one.
    /// The snapshot is rewritten only once the journal has grown past
    /// [`COMPACT_AFTER`], or when there is no snapshot yet.
    pub fn save(&self, state: &mut SyncState) -> Result<()> {
        let pending = std::mem::take(&mut state.pending);
        if pending.is_empty() && self.path.exists() {
            return Ok(());
        }
        let journal = self.journal();
        let folded = journal.record_count().unwrap_or(0) + pending.len();
        if !self.path.exists() || folded >= COMPACT_AFTER {
            return self.compact(state);
        }
        match journal.append(&pending) {
            Ok(()) => Ok(()),
            // Losing the journal is survivable; losing the change is not. Fall
            // back to the whole-file write rather than dropping it.
            Err(error) => {
                tracing::warn!(%error, "could not append to the state journal; rewriting it whole");
                self.compact(state)
            }
        }
    }

    /// Fold the journal into a fresh snapshot and drop it.
    ///
    /// The order is load-bearing: the snapshot is renamed into place *before*
    /// the journal is cleared, so a crash between the two replays records the
    /// snapshot already contains — which is harmless, because replaying a
    /// record twice lands on the same state — rather than losing them.
    fn compact(&self, state: &SyncState) -> Result<()> {
        self.write_snapshot(state)?;
        self.journal().clear()
    }

    /// Write the state so that a crash leaves either the old file or the new
    /// one, never a partial one.
    ///
    /// The file is synced before the rename so its bytes are durable, and the
    /// directory is synced after so the rename itself survives power loss.
    fn write_snapshot(&self, state: &SyncState) -> Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| Error::Config("state path has no parent directory".into()))?;
        std::fs::create_dir_all(parent)?;

        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::Config(format!("cannot serialise sync state: {e}")))?;

        let temp = self.path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&temp, &self.path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> (tempfile::TempDir, StateDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = StateDb::at(dir.path().join("nested").join("state.json"));
        (dir, db)
    }

    fn entry(path: &str) -> SyncEntry {
        SyncEntry {
            rev: "r1".into(),
            content_hash: "h1".into(),
            mtime_nanos: 1_700_000_000_000_000_000,
            size: 7,
            display_path: path.into(),
        }
    }

    #[test]
    fn a_missing_file_loads_as_empty_state() {
        let (_dir, db) = db();
        let state = db.load().unwrap();
        assert!(state.is_empty());
        assert_eq!(state.cursor(), None);
    }

    #[test]
    fn entries_and_cursor_round_trip_through_disk() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.set_cursor("cursor-abc");
        state.insert(entry("/Photos/Cat.JPG"));
        db.save(&mut state).unwrap();

        let loaded = db.load().unwrap();
        assert_eq!(loaded.cursor(), Some("cursor-abc"));
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded.get("/Photos/Cat.JPG"),
            Some(&entry("/Photos/Cat.JPG"))
        );
    }

    /// Dropbox paths are case-insensitive: a lookup with different casing must
    /// find the same file, not miss and cause a duplicate download.
    #[test]
    fn lookups_are_case_insensitive() {
        let mut state = SyncState::new();
        state.insert(entry("/Photos/Cat.JPG"));
        assert!(state.get("/photos/cat.jpg").is_some());
        assert!(state.get("/PHOTOS/CAT.JPG").is_some());
    }

    /// Re-inserting under different casing must replace, not add a second row.
    #[test]
    fn a_case_change_replaces_rather_than_duplicates() {
        let mut state = SyncState::new();
        state.insert(entry("/Photos/Cat.JPG"));
        state.insert(entry("/photos/cat.jpg"));
        assert_eq!(state.len(), 1);
        assert_eq!(
            state.entries().next().unwrap().display_path,
            "/photos/cat.jpg"
        );
    }

    #[test]
    fn the_original_casing_is_preserved_for_display() {
        let mut state = SyncState::new();
        state.insert(entry("/Photos/Cat.JPG"));
        assert_eq!(
            state.get("/photos/cat.jpg").unwrap().display_path,
            "/Photos/Cat.JPG"
        );
    }

    #[test]
    fn removal_is_case_insensitive_too() {
        let mut state = SyncState::new();
        state.insert(entry("/Photos/Cat.JPG"));
        assert!(state.remove("/photos/cat.jpg").is_some());
        assert!(state.is_empty());
    }

    /// Clearing the cursor must not discard what we know about the files, or a
    /// cursor reset would turn into a full re-download.
    #[test]
    fn clearing_the_cursor_keeps_the_entries() {
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        state.set_cursor("c");
        state.clear_cursor();
        assert_eq!(state.cursor(), None);
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn saving_twice_replaces_the_previous_state() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();

        state.remove("/a.txt");
        state.insert(entry("/b.txt"));
        db.save(&mut state).unwrap();

        let loaded = db.load().unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.get("/b.txt").is_some());
    }

    #[test]
    fn no_temp_file_survives_a_save() {
        let (_dir, db) = db();
        db.save(&mut SyncState::new()).unwrap();
        assert!(!db.path().with_extension("tmp").exists());
    }

    /// A state file from a newer build must be refused, not misinterpreted.
    #[test]
    fn a_future_state_version_is_rejected() {
        let (_dir, db) = db();
        std::fs::create_dir_all(db.path().parent().unwrap()).unwrap();
        std::fs::write(db.path(), r#"{"version": 999, "entries": {}}"#).unwrap();
        assert!(matches!(db.load(), Err(Error::Config(_))));
    }

    /// A crash midway through a save leaves a stale `.tmp`. The real state
    /// file is still the old, complete one, and loading must ignore the
    /// leftover entirely.
    #[test]
    fn a_leftover_temp_file_from_a_crashed_save_is_ignored() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();

        std::fs::write(db.path().with_extension("tmp"), "{ half-written").unwrap();

        let loaded = db.load().unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.get("/a.txt").is_some());
    }

    #[test]
    fn corrupt_json_is_an_error_rather_than_silent_data_loss() {
        let (_dir, db) = db();
        std::fs::create_dir_all(db.path().parent().unwrap()).unwrap();
        std::fs::write(db.path(), "{not json").unwrap();
        assert!(db.load().is_err());
    }

    #[test]
    fn entries_iterate_in_stable_key_order() {
        let mut state = SyncState::new();
        for path in ["/c.txt", "/a.txt", "/b.txt"] {
            state.insert(entry(path));
        }
        let paths: Vec<_> = state.entries().map(|e| e.display_path.as_str()).collect();
        assert_eq!(paths, ["/a.txt", "/b.txt", "/c.txt"]);
    }

    /// The whole point: a save must cost what changed, not what is stored.
    /// Rewriting every entry per checkpoint is what made a large pull quadratic.
    #[test]
    fn a_save_after_the_first_appends_instead_of_rewriting() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();
        let snapshot = std::fs::metadata(db.path()).unwrap().modified().unwrap();

        state.insert(entry("/b.txt"));
        db.save(&mut state).unwrap();

        assert_eq!(
            std::fs::metadata(db.path()).unwrap().modified().unwrap(),
            snapshot,
            "the snapshot was not touched"
        );
        assert_eq!(db.journal().record_count().unwrap(), 1);
    }

    /// A journal is only useful if loading applies it.
    #[test]
    fn a_load_replays_the_journal_on_top_of_the_snapshot() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();
        state.insert(entry("/b.txt"));
        state.set_cursor("c2");
        state.remove("/a.txt");
        db.save(&mut state).unwrap();

        let loaded = db.load().unwrap();

        assert!(loaded.get("/b.txt").is_some(), "the appended entry");
        assert!(loaded.get("/a.txt").is_none(), "the appended removal");
        assert_eq!(loaded.cursor(), Some("c2"));
    }

    /// Otherwise the journal would grow without bound and startup with it.
    #[test]
    fn the_journal_is_folded_back_into_the_snapshot_once_it_is_long() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();

        for i in 0..COMPACT_AFTER {
            state.insert(entry(&format!("/f{i}.txt")));
        }
        db.save(&mut state).unwrap();

        assert_eq!(db.journal().record_count().unwrap(), 0, "folded in");
        assert_eq!(db.load().unwrap().len(), COMPACT_AFTER + 1);
    }

    /// Replaying a record the snapshot already contains must be harmless: a
    /// crash between writing the snapshot and clearing the journal leaves
    /// exactly that, and losing the records would be the worse trade.
    #[test]
    fn replaying_a_record_the_snapshot_already_holds_changes_nothing() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();
        // The snapshot holds /a.txt; put the same record in the journal too.
        db.journal()
            .append(&[Record::Entry(Box::new(entry("/a.txt")))])
            .unwrap();

        let loaded = db.load().unwrap();

        assert_eq!(loaded.len(), 1);
        assert!(loaded.get("/a.txt").is_some());
    }

    /// A load hands back state that matches the disk, so an immediate save must
    /// not append the records it just replayed all over again.
    #[test]
    fn a_load_leaves_nothing_pending() {
        let (_dir, db) = db();
        let mut state = SyncState::new();
        state.insert(entry("/a.txt"));
        db.save(&mut state).unwrap();
        state.insert(entry("/b.txt"));
        db.save(&mut state).unwrap();

        let mut loaded = db.load().unwrap();
        db.save(&mut loaded).unwrap();

        assert_eq!(db.journal().record_count().unwrap(), 1, "unchanged");
    }
}
