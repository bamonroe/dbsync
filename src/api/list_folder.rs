//! Folder listing and the remote change stream.
//!
//! `list_folder` opens the stream and `list_folder/continue` advances it. Both
//! return a cursor, and that cursor is the daemon's whole position in the
//! remote: it is what the long-poll endpoint parks on and what the state
//! database persists.

use serde::Serialize;

use super::client::ApiClient;
use super::metadata::ListFolderPage;
use crate::error::Result;

#[derive(Serialize)]
struct ListFolderRequest<'a> {
    path: &'a str,
    recursive: bool,
    /// Tombstones are how a delete reaches us; without this the stream would
    /// silently skip them.
    include_deleted: bool,
}

#[derive(Serialize)]
struct ContinueRequest<'a> {
    cursor: &'a str,
}

impl ApiClient {
    /// Open a recursive listing of `path` (the empty string means the root).
    pub async fn list_folder(&self, path: &str) -> Result<ListFolderPage> {
        self.rpc(
            "files/list_folder",
            &ListFolderRequest {
                path,
                recursive: true,
                include_deleted: true,
            },
        )
        .await
    }

    /// Fetch the changes since `cursor`.
    ///
    /// Returns [`crate::Error::CursorReset`] when Dropbox has invalidated the
    /// cursor; the caller must then re-list and reconcile from scratch.
    pub async fn list_folder_continue(&self, cursor: &str) -> Result<ListFolderPage> {
        self.rpc("files/list_folder/continue", &ContinueRequest { cursor })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The listing must be recursive and must include tombstones, or the
    /// daemon would never see a delete or anything below the top level.
    #[test]
    fn a_listing_asks_for_recursion_and_tombstones() {
        let json = serde_json::to_value(ListFolderRequest {
            path: "/Work",
            recursive: true,
            include_deleted: true,
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({"path": "/Work", "recursive": true, "include_deleted": true})
        );
    }

    #[test]
    fn continue_sends_only_the_cursor() {
        let json = serde_json::to_value(ContinueRequest { cursor: "AAE" }).unwrap();
        assert_eq!(json, serde_json::json!({"cursor": "AAE"}));
    }
}
