//! Typed wrappers over the authenticated Dropbox endpoints.
//!
//! Covers `api.dropboxapi.com` (metadata, `list_folder`, `list_folder/continue`)
//! and `content.dropboxapi.com` (upload, download, upload sessions). The
//! unauthenticated notification endpoint deliberately lives elsewhere, in
//! [`crate::notify`].
//!
//! Not yet implemented — see the remote/local sync tasks in `TODO.toml`.
