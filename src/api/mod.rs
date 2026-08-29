//! Typed wrappers over the authenticated Dropbox endpoints.
//!
//! Covers `api.dropboxapi.com` (metadata, `list_folder`, `list_folder/continue`)
//! and `content.dropboxapi.com` (upload, download, upload sessions). The
//! unauthenticated notification endpoint deliberately lives elsewhere, in
//! [`crate::notify`], because it takes no `Authorization` header at all.
//!
//! The split within this module follows the split in the API itself: one file
//! for the client and its error mapping, one for the metadata shapes, and one
//! per endpoint family.

mod chunkmap;
mod chunks;
mod client;
mod download;
mod list_folder;
mod metadata;
mod range;
mod upload;

pub use client::ApiClient;
pub use download::is_partial;
pub use metadata::{ListFolderPage, RemoteDeleted, RemoteEntry, RemoteFile, RemoteFolder};
pub use upload::{CHUNK_SIZE, SESSION_THRESHOLD, WriteMode};
