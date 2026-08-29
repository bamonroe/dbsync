//! The remote half of the reconciler, abstracted.
//!
//! The applier is written against this trait rather than against
//! [`crate::api::ApiClient`] so its logic — what to write, what to delete, when
//! to skip — can be tested against a fake account instead of the network.

use std::future::Future;
use std::path::Path;

use crate::api::{ApiClient, ListFolderPage};
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

    /// Download revision `rev` of `remote_path`, atomically placing it at
    /// `dest`. The revision is what makes an interrupted download resumable.
    ///
    /// `size` is the revision's length, which the listing already carries and
    /// the byte budget already spends. Passing it means a download can plan
    /// its byte ranges up front rather than discovering the length by
    /// reaching the end of the stream.
    fn download_to(
        &self,
        remote_path: &str,
        rev: &str,
        size: u64,
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

    fn download_to(
        &self,
        remote_path: &str,
        rev: &str,
        size: u64,
        dest: &Path,
    ) -> impl Future<Output = Result<()>> + Send {
        ApiClient::download_to(self, remote_path, rev, size, dest)
    }
}
