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
        }
    }

    /// Our position in the remote change stream, if we have one.
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Advance the cursor after applying a batch of changes.
    pub fn set_cursor(&mut self, cursor: impl Into<String>) {
        self.cursor = Some(cursor.into());
    }

    /// Forget the cursor. Called when Dropbox resets it, which forces a
    /// re-list; the entries stay, so the re-list reconciles rather than
    /// re-downloads.
    pub fn clear_cursor(&mut self) {
        self.cursor = None;
    }

    /// The entry for a path, matched case-insensitively.
    pub fn get(&self, path: &str) -> Option<&SyncEntry> {
        self.entries.get(&key_for(path))
    }

    /// Record or replace the entry for a path.
    pub fn insert(&mut self, entry: SyncEntry) {
        self.entries.insert(key_for(&entry.display_path), entry);
    }

    /// Forget a path, returning what was there.
    pub fn remove(&mut self, path: &str) -> Option<SyncEntry> {
        self.entries.remove(&key_for(path))
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
        self.failures
            .entry(key_for(path))
            .and_modify(|failure| failure.record_again(text.clone(), kind, direction))
            .or_insert_with(|| Failure::new(path, text, kind, direction));
    }

    /// Forget any failure for `path`. Called on every success, so a file that
    /// later arrives stops being reported as missing.
    pub fn clear_failure(&mut self, path: &str) -> Option<Failure> {
        self.failures.remove(&key_for(path))
    }

    /// Record a prepared failure verbatim. For tests and for migrating a
    /// record between keys; ordinary recording goes through
    /// [`Self::record_failure`], which folds attempts and timestamps.
    pub fn insert_failure(&mut self, path: &str, failure: Failure) {
        self.failures.insert(key_for(path), failure);
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

    /// Load the state, or a fresh empty one if this is the first run.
    pub fn load(&self) -> Result<SyncState> {
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

    /// Write the state so that a crash leaves either the old file or the new
    /// one, never a partial one.
    ///
    /// The file is synced before the rename so its bytes are durable, and the
    /// directory is synced after so the rename itself survives power loss.
    pub fn save(&self, state: &SyncState) -> Result<()> {
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
        db.save(&state).unwrap();

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
        db.save(&state).unwrap();

        state.remove("/a.txt");
        state.insert(entry("/b.txt"));
        db.save(&state).unwrap();

        let loaded = db.load().unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.get("/b.txt").is_some());
    }

    #[test]
    fn no_temp_file_survives_a_save() {
        let (_dir, db) = db();
        db.save(&SyncState::new()).unwrap();
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
        db.save(&state).unwrap();

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
}
