//! A fake Dropbox account, for testing the applier without a network.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use crate::api::ListFolderPage;
use crate::error::{Error, Result};

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
    cursors_used: Mutex<Vec<String>>,
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

    async fn download_to(&self, remote_path: &str, dest: &Path) -> Result<()> {
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
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(dest, &content).await?;
        Ok(())
    }
}
