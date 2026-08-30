//! Metadata for one path.
//!
//! The listing endpoints answer "what changed?"; this answers "what is this
//! path now?". Retrying a single failed download needs the second question: the
//! entry that failed came from a page that has long since been consumed, and
//! the cursor cannot be rewound to it.

use serde::Serialize;

use super::client::ApiClient;
use super::metadata::RemoteEntry;
use crate::error::Result;

#[derive(Serialize)]
struct GetMetadataRequest<'a> {
    path: &'a str,
    /// A path that has since been deleted must come back as a tombstone rather
    /// than an error, so a retry can tell "gone" from "broken" and stop
    /// retrying something that is no longer there.
    include_deleted: bool,
}

impl ApiClient {
    /// Look up one path's current metadata.
    pub async fn get_metadata(&self, path: &str) -> Result<RemoteEntry> {
        self.rpc(
            "files/get_metadata",
            &GetMetadataRequest {
                path,
                include_deleted: true,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without `include_deleted` a retry of a since-deleted path would look
    /// like a failure rather than a reason to stop retrying.
    #[test]
    fn a_lookup_asks_for_tombstones() {
        let json = serde_json::to_value(GetMetadataRequest {
            path: "/Work/report.pdf",
            include_deleted: true,
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({"path": "/Work/report.pdf", "include_deleted": true})
        );
    }
}
