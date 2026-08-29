//! The metadata shapes Dropbox returns from `list_folder` and friends.
//!
//! Dropbox tags its union types with a `.tag` field, so a folder listing is a
//! heterogeneous array of files, folders, and tombstones. Modelling that as an
//! enum means the reconciler matches on the three cases instead of sniffing
//! optional fields.

use serde::Deserialize;

/// One item in a folder listing or change stream.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = ".tag", rename_all = "snake_case")]
pub enum RemoteEntry {
    File(RemoteFile),
    Folder(RemoteFolder),
    /// A tombstone: the path is gone (deleted, or moved away).
    Deleted(RemoteDeleted),
}

impl RemoteEntry {
    /// The case-preserved path, as Dropbox would display it.
    ///
    /// Tombstones sometimes arrive without one, in which case the lowercased
    /// path is the only thing on offer.
    pub fn display_path(&self) -> &str {
        match self {
            Self::File(file) => &file.path_display,
            Self::Folder(folder) => &folder.path_display,
            Self::Deleted(deleted) => deleted
                .path_display
                .as_deref()
                .unwrap_or(&deleted.path_lower),
        }
    }

    /// The lowercased path. Dropbox is case-insensitive, so this — not the
    /// display path — is the identity of an item.
    pub fn path_lower(&self) -> &str {
        match self {
            Self::File(file) => &file.path_lower,
            Self::Folder(folder) => &folder.path_lower,
            Self::Deleted(deleted) => &deleted.path_lower,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RemoteFile {
    pub path_lower: String,
    pub path_display: String,
    /// The revision. Changes on every edit; this is what we compare against
    /// the local state to spot an echo of our own upload.
    pub rev: String,
    pub size: u64,
    /// Dropbox's content hash — the same 4 MiB SHA-256 tree [`crate::state::hash`]
    /// computes locally.
    #[serde(default)]
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RemoteFolder {
    pub path_lower: String,
    pub path_display: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RemoteDeleted {
    pub path_lower: String,
    #[serde(default)]
    pub path_display: Option<String>,
}

/// One page of a folder listing or change stream.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ListFolderPage {
    pub entries: Vec<RemoteEntry>,
    /// Feed this to `list_folder/continue` — or to the long-poll endpoint.
    pub cursor: String,
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(json: &str) -> ListFolderPage {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_listing_parses_all_three_kinds_of_entry() {
        let parsed = page(
            r#"{
                "entries": [
                    {".tag": "folder", "path_lower": "/docs", "path_display": "/Docs"},
                    {".tag": "file", "path_lower": "/docs/a.txt", "path_display": "/Docs/A.txt",
                     "rev": "0159", "size": 12, "content_hash": "abc"},
                    {".tag": "deleted", "path_lower": "/old.txt", "path_display": "/Old.txt"}
                ],
                "cursor": "AAE", "has_more": false
            }"#,
        );

        assert_eq!(parsed.cursor, "AAE");
        assert!(!parsed.has_more);
        assert!(matches!(parsed.entries[0], RemoteEntry::Folder(_)));
        assert!(matches!(parsed.entries[1], RemoteEntry::File(_)));
        assert!(matches!(parsed.entries[2], RemoteEntry::Deleted(_)));
    }

    /// Unknown fields are routine — Dropbox adds them — and must not fail the
    /// whole page.
    #[test]
    fn unexpected_fields_are_ignored() {
        let parsed = page(
            r#"{"entries": [{".tag": "file", "path_lower": "/a", "path_display": "/a",
                 "rev": "1", "size": 1, "id": "id:x", "server_modified": "2020-01-01T00:00:00Z"}],
                "cursor": "c", "has_more": true}"#,
        );
        assert!(parsed.has_more);
        assert_eq!(parsed.entries[0].path_lower(), "/a");
    }

    /// A tombstone without a display path still has to name itself.
    #[test]
    fn a_tombstone_falls_back_to_its_lowercased_path() {
        let entry: RemoteEntry =
            serde_json::from_str(r#"{".tag": "deleted", "path_lower": "/gone.txt"}"#).unwrap();
        assert_eq!(entry.display_path(), "/gone.txt");
    }

    #[test]
    fn a_file_keeps_the_case_dropbox_displays() {
        let entry: RemoteEntry = serde_json::from_str(
            r#"{".tag": "file", "path_lower": "/a/b.txt", "path_display": "/A/B.txt",
                "rev": "9", "size": 3}"#,
        )
        .unwrap();
        assert_eq!(entry.display_path(), "/A/B.txt");
        assert_eq!(entry.path_lower(), "/a/b.txt");
    }
}
