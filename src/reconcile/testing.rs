//! A fake Dropbox account, and the entry builders that stage it, for testing
//! the reconciler without a network.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use crate::api::{
    Allowance, ListFolderPage, RemoteDeleted, RemoteEntry, RemoteFile, RemoteFolder, WriteMode,
};
use crate::error::{Error, Result};
use crate::state::SyncState;

use super::paths::PathMapper;
use super::sink::RemoteSink;
use super::source::RemoteSource;

/// A remote file at `rev`, sized zero and unhashed.
///
/// The size and hash are what a *download* consults; every test that builds an
/// entry by hand is exercising the routing above that, so they are left empty
/// rather than made another knob to pass.
pub fn file(path: &str, rev: &str) -> RemoteEntry {
    sized_file(path, rev, 0)
}

/// The same, with the size a listing claims — what admission control reads.
pub fn sized_file(path: &str, rev: &str, size: u64) -> RemoteEntry {
    RemoteEntry::File(RemoteFile {
        path_lower: path.to_lowercase(),
        path_display: path.to_string(),
        rev: rev.to_string(),
        size,
        content_hash: None,
    })
}

/// A remote folder entry.
pub fn folder(path: &str) -> RemoteEntry {
    RemoteEntry::Folder(RemoteFolder {
        path_lower: path.to_lowercase(),
        path_display: path.to_string(),
    })
}

/// A remote delete marker — what a listing carries in place of a removed path.
pub fn tombstone(path: &str) -> RemoteEntry {
    RemoteEntry::Deleted(RemoteDeleted {
        path_lower: path.to_lowercase(),
        path_display: Some(path.to_string()),
    })
}

/// One listing page, ending at `cursor`.
pub fn page(entries: Vec<RemoteEntry>, cursor: &str, has_more: bool) -> ListFolderPage {
    ListFolderPage {
        entries,
        cursor: cursor.to_string(),
        has_more,
    }
}

/// A temp directory, a fake account, and the sync state that ties them —
/// everything the single-path helpers (`apply_entry`, `push_path`) need.
///
/// The per-module verbs live in each test module as extra inherent impls, so
/// this holds only what they all share.
pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub remote: FakeRemote,
    pub state: SyncState,
}

impl Fixture {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            remote: FakeRemote::new(),
            state: SyncState::new(),
        }
    }

    /// A mapper rooted at the temp directory, with the whole account synced.
    pub fn paths(&self) -> PathMapper {
        PathMapper::new(self.dir.path(), "")
    }

    /// Where `relative` sits on disk.
    pub fn local(&self, relative: &str) -> std::path::PathBuf {
        self.dir.path().join(relative)
    }

    /// Write `content` to `relative`, creating parents, and return its path.
    pub fn write(&self, relative: &str, content: &[u8]) -> std::path::PathBuf {
        let path = self.local(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, content).unwrap();
        path
    }
}

/// An in-memory stand-in for the remote side.
///
/// Holds file contents to serve downloads, plus a queue of scripted listing
/// pages so a test can stage paging, tombstones, and cursor resets.
#[derive(Default)]
pub struct FakeRemote {
    files: Mutex<HashMap<String, Vec<u8>>>,
    listings: Mutex<VecDeque<Result<ListFolderPage>>>,
    continues: Mutex<VecDeque<Result<ListFolderPage>>>,
    downloads: Mutex<usize>,
    /// Downloads currently being served, and the most there have ever been at
    /// once — how a test sees that fetches really did overlap.
    in_flight: Mutex<(usize, usize)>,
    /// Extra yields to make a named path wait for before it is served, so a
    /// test can force a page to complete in an order other than its listing's.
    stalls: Mutex<HashMap<String, usize>>,
    /// Every download that finished, in completion order.
    completed: Mutex<Vec<String>>,
    /// The path and expected size of every download, in call order, so a test
    /// can see the listing's size really reached the fetch.
    sizes_asked: Mutex<Vec<(String, u64)>>,
    cursors_used: Mutex<Vec<String>>,
    uploads: Mutex<Vec<WriteMode>>,
    deletes: Mutex<usize>,
    /// Bumped on every upload so each write gets a distinct revision, the way
    /// Dropbox would.
    revision: Mutex<u64>,
    /// Remote paths whose next `update(rev)` write is refused, the way Dropbox
    /// refuses one naming a revision that is no longer current.
    conflicts: Mutex<HashMap<String, usize>>,
    /// Remote paths whose next `count` uploads fail outright, so a test can
    /// see a failed upload recorded rather than merely logged.
    upload_failures: Mutex<HashMap<String, usize>>,
    /// Metadata a test has queued for `get_metadata`, keyed by lowercased path.
    /// A path with no entry here is reported gone, which is what a retry of a
    /// since-deleted file should see.
    metadata: Mutex<HashMap<String, RemoteEntry>>,
    /// Every path `get_metadata` was asked about, in order.
    metadata_asked: Mutex<Vec<String>>,
}

impl FakeRemote {
    pub fn new() -> Self {
        Self::default()
    }

    /// Put a file in the fake account so a download can serve it.
    pub fn put(&self, path: &str, content: &[u8]) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_lowercase(), content.to_vec());
    }

    /// Queue a page for the next `list_folder` call.
    /// Make the next `count` updates to `path` fail with a conflict.
    /// Make the next `count` uploads of `path` fail, the way a dropped
    /// connection would.
    pub fn fail_uploads(&self, path: &str, count: usize) {
        self.upload_failures
            .lock()
            .unwrap()
            .insert(path.to_lowercase(), count);
    }

    pub fn refuse_updates(&self, path: &str, count: usize) {
        self.conflicts
            .lock()
            .unwrap()
            .insert(path.to_lowercase(), count);
    }

    pub fn queue_listing(&self, page: Result<ListFolderPage>) {
        self.listings.lock().unwrap().push_back(page);
    }

    /// Queue a page for the next `list_folder/continue` call.
    pub fn queue_continue(&self, page: Result<ListFolderPage>) {
        self.continues.lock().unwrap().push_back(page);
    }

    /// How many downloads have been served — the check for "did it skip?".
    pub fn downloads(&self) -> usize {
        *self.downloads.lock().unwrap()
    }

    /// The most downloads that were ever in flight at one time.
    pub fn peak_in_flight(&self) -> usize {
        self.in_flight.lock().unwrap().1
    }

    /// Hold downloads of `path` back until `others` other downloads have
    /// finished, so it lands last however the runtime schedules them.
    ///
    /// Counting completions rather than yields is what makes this
    /// deterministic: a download's final step is a real filesystem write, and
    /// how many times that await reschedules is not fixed, so a stall measured
    /// in yields loses the race intermittently.
    pub fn stall(&self, path: &str, others: usize) {
        self.stalls
            .lock()
            .unwrap()
            .insert(path.to_lowercase(), others);
    }

    /// Which downloads finished, in the order they actually finished.
    pub fn completed(&self) -> Vec<String> {
        self.completed.lock().unwrap().clone()
    }

    /// What a path holds in the fake account, if anything.
    pub fn content(&self, path: &str) -> Option<Vec<u8>> {
        self.files
            .lock()
            .unwrap()
            .get(&path.to_lowercase())
            .cloned()
    }

    /// The write mode of each upload, in order.
    pub fn modes(&self) -> Vec<WriteMode> {
        self.uploads.lock().unwrap().clone()
    }

    pub fn uploads(&self) -> usize {
        self.uploads.lock().unwrap().len()
    }

    pub fn deletes(&self) -> usize {
        *self.deletes.lock().unwrap()
    }

    /// The size each download was told to expect, keyed by path in call order.
    pub fn sizes_asked(&self) -> Vec<(String, u64)> {
        self.sizes_asked.lock().unwrap().clone()
    }

    /// Every cursor `continue` was called with, in order.
    pub fn cursors_used(&self) -> Vec<String> {
        self.cursors_used.lock().unwrap().clone()
    }

    /// Queue what `get_metadata` should answer for a path.
    pub fn set_metadata(&self, entry: RemoteEntry) {
        self.metadata
            .lock()
            .unwrap()
            .insert(entry.display_path().to_lowercase(), entry);
    }

    /// Every path `get_metadata` was asked about, in order.
    pub fn metadata_asked(&self) -> Vec<String> {
        self.metadata_asked.lock().unwrap().clone()
    }
}

fn next(queue: &Mutex<VecDeque<Result<ListFolderPage>>>, what: &str) -> Result<ListFolderPage> {
    queue
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| panic!("no {what} page queued"))
}

impl RemoteSource for FakeRemote {
    async fn list_folder(&self, _path: &str) -> Result<ListFolderPage> {
        next(&self.listings, "listing")
    }

    async fn get_metadata(&self, path: &str) -> Result<RemoteEntry> {
        self.metadata_asked.lock().unwrap().push(path.to_string());
        self.metadata
            .lock()
            .unwrap()
            .get(&path.to_lowercase())
            .cloned()
            .ok_or_else(|| Error::Api {
                status: 409,
                message: format!("path/not_found: {path}"),
            })
    }

    async fn list_folder_continue(&self, cursor: &str) -> Result<ListFolderPage> {
        self.cursors_used.lock().unwrap().push(cursor.to_string());
        next(&self.continues, "continue")
    }

    async fn download_to(
        &self,
        remote_path: &str,
        _rev: &str,
        allowance: Allowance,
        _expected_hash: Option<&str>,
        dest: &Path,
    ) -> Result<()> {
        self.sizes_asked
            .lock()
            .unwrap()
            .push((remote_path.to_string(), allowance.size));
        let content = self
            .files
            .lock()
            .unwrap()
            .get(&remote_path.to_lowercase())
            .cloned();
        let Some(content) = content else {
            return Err(Error::Api {
                status: 409,
                message: format!("path_not_found: {remote_path}"),
            });
        };
        *self.downloads.lock().unwrap() += 1;
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            in_flight.0 += 1;
            in_flight.1 = in_flight.1.max(in_flight.0);
        }
        // Serving without ever yielding would make every download finish before
        // the next one is polled, so a fake account would look sequential
        // however concurrent the caller is.
        tokio::task::yield_now().await;
        let stall = self
            .stalls
            .lock()
            .unwrap()
            .get(&remote_path.to_lowercase())
            .copied()
            .unwrap_or(0);
        // Bounded so a stall that can never be satisfied fails the test rather
        // than hanging the suite.
        for _ in 0..10_000 {
            if self.completed.lock().unwrap().len() >= stall {
                break;
            }
            tokio::task::yield_now().await;
        }
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(dest, &content).await?;
        self.in_flight.lock().unwrap().0 -= 1;
        self.completed.lock().unwrap().push(remote_path.to_string());
        Ok(())
    }
}

impl RemoteSink for FakeRemote {
    async fn upload(
        &self,
        remote_path: &str,
        local: &Path,
        mode: &WriteMode,
    ) -> Result<RemoteFile> {
        if let Some(left) = self
            .upload_failures
            .lock()
            .unwrap()
            .get_mut(&remote_path.to_lowercase())
            && *left > 0
        {
            *left -= 1;
            return Err(Error::Api {
                status: 503,
                message: "upload failed".into(),
            });
        }
        if let Some(left) = self
            .conflicts
            .lock()
            .unwrap()
            .get_mut(&remote_path.to_lowercase())
            && *left > 0
            && matches!(mode, WriteMode::Update(_))
        {
            *left -= 1;
            return Err(Error::Conflict);
        }
        let content = tokio::fs::read(local).await?;
        self.files
            .lock()
            .unwrap()
            .insert(remote_path.to_lowercase(), content.clone());
        self.uploads.lock().unwrap().push(mode.clone());
        let rev = {
            let mut revision = self.revision.lock().unwrap();
            *revision += 1;
            format!("r{revision}")
        };
        Ok(RemoteFile {
            path_lower: remote_path.to_lowercase(),
            path_display: remote_path.to_string(),
            rev,
            size: content.len() as u64,
            content_hash: Some(crate::state::hash::hash_bytes(&content)),
        })
    }

    async fn delete(&self, remote_path: &str) -> Result<()> {
        self.files
            .lock()
            .unwrap()
            .remove(&remote_path.to_lowercase());
        *self.deletes.lock().unwrap() += 1;
        Ok(())
    }
}
