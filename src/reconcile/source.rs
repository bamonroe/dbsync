//! The remote half of the reconciler, abstracted.
//!
//! The applier is written against this trait rather than against
//! [`crate::api::ApiClient`] so its logic — what to write, what to delete, when
//! to skip — can be tested against a fake account instead of the network.

use std::future::Future;
use std::path::Path;

use crate::api::{Allowance, ApiClient, ListFolderPage, RemoteEntry};
use crate::error::Result;

/// Everything the remote-to-local direction needs from Dropbox.
pub trait RemoteSource {
    /// Open a recursive listing of `path`.
    fn list_folder(&self, path: &str) -> impl Future<Output = Result<ListFolderPage>> + Send;

    /// Fetch the changes since `cursor`.
    fn list_folder_continue(
        &self,
        cursor: &str,
    ) -> impl Future<Output = Result<ListFolderPage>> + Send;

    /// Look up one path's current metadata.
    ///
    /// Retrying a single failed entry needs this: the page it arrived on is
    /// long consumed, and a Dropbox cursor cannot be rewound to it.
    fn get_metadata(&self, path: &str) -> impl Future<Output = Result<RemoteEntry>> + Send;

    /// Download revision `rev` of `remote_path`, atomically placing it at
    /// `dest`. The revision is what makes an interrupted download resumable.
    ///
    /// `allowance` carries the revision's length and how many of its bytes the
    /// caller's budget reserved: the first plans the chunk layout, the second
    /// bounds how many of those chunks may be in flight, so per-file and
    /// across-file parallelism compose instead of multiplying.
    fn download_to(
        &self,
        remote_path: &str,
        rev: &str,
        allowance: Allowance,
        dest: &Path,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl RemoteSource for ApiClient {
    fn list_folder(&self, path: &str) -> impl Future<Output = Result<ListFolderPage>> + Send {
        ApiClient::list_folder(self, path)
    }

    fn list_folder_continue(
        &self,
        cursor: &str,
    ) -> impl Future<Output = Result<ListFolderPage>> + Send {
        ApiClient::list_folder_continue(self, cursor)
    }

    fn get_metadata(&self, path: &str) -> impl Future<Output = Result<RemoteEntry>> + Send {
        ApiClient::get_metadata(self, path)
    }

    fn download_to(
        &self,
        remote_path: &str,
        rev: &str,
        allowance: Allowance,
        dest: &Path,
    ) -> impl Future<Output = Result<()>> + Send {
        ApiClient::download_to(self, remote_path, rev, allowance, dest)
    }
}
