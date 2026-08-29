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

    /// Download `remote_path`, atomically placing it at `dest`.
    fn download_to(
        &self,
        remote_path: &str,
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
        dest: &Path,
    ) -> impl Future<Output = Result<()>> + Send {
        ApiClient::download_to(self, remote_path, dest)
    }
}
