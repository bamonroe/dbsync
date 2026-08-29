//! A fake Dropbox account, for testing the applier without a network.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use crate::api::{ListFolderPage, RemoteFile, WriteMode};
use crate::error::{Error, Result};

use super::sink::RemoteSink;
use super::source::RemoteSource;

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
    cursors_used: Mutex<Vec<String>>,
    uploads: Mutex<Vec<WriteMode>>,
    deletes: Mutex<usize>,
    /// Bumped on every upload so each write gets a distinct revision, the way
    /// Dropbox would.
    revision: Mutex<u64>,
    /// Remote paths whose next `update(rev)` write is refused, the way Dropbox
    /// refuses one naming a revision that is no longer current.
    conflicts: Mutex<HashMap<String, usize>>,
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

    /// Hold downloads of `path` back for `rounds` extra yields, so it finishes
    /// after files listed behind it.
    pub fn stall(&self, path: &str, rounds: usize) {
        self.stalls
            .lock()
            .unwrap()
            .insert(path.to_lowercase(), rounds);
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

    /// Every cursor `continue` was called with, in order.
    pub fn cursors_used(&self) -> Vec<String> {
        self.cursors_used.lock().unwrap().clone()
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

    async fn list_folder_continue(&self, cursor: &str) -> Result<ListFolderPage> {
        self.cursors_used.lock().unwrap().push(cursor.to_string());
        next(&self.continues, "continue")
    }

    async fn download_to(&self, remote_path: &str, _rev: &str, dest: &Path) -> Result<()> {
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
        for _ in 0..stall {
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
