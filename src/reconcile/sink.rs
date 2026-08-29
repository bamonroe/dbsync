//! The upload half of the remote, abstracted.
//!
//! The counterpart to [`super::source::RemoteSource`]: everything the
//! local-to-remote direction needs, expressed as a trait so the push logic can
//! be tested against an in-memory account.

use std::future::Future;
use std::path::Path;

use crate::api::{ApiClient, RemoteFile, WriteMode};
use crate::error::Result;

/// Everything the local-to-remote direction needs from Dropbox.
pub trait RemoteSink {
    /// Upload `local` to `remote_path`, returning the metadata Dropbox stored.
    fn upload(
        &self,
        remote_path: &str,
        local: &Path,
        mode: &WriteMode,
    ) -> impl Future<Output = Result<RemoteFile>> + Send;

    /// Delete a remote path. Already-gone is success.
    fn delete(&self, remote_path: &str) -> impl Future<Output = Result<()>> + Send;
}

impl RemoteSink for ApiClient {
    fn upload(
        &self,
        remote_path: &str,
        local: &Path,
        mode: &WriteMode,
    ) -> impl Future<Output = Result<RemoteFile>> + Send {
        ApiClient::upload(self, remote_path, local, mode)
    }

    fn delete(&self, remote_path: &str) -> impl Future<Output = Result<()>> + Send {
        ApiClient::delete(self, remote_path)
    }
}
