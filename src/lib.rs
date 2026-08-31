//! dbsync — a realtime Dropbox sync daemon for Linux.
//!
//! Remote changes arrive by push: the daemon parks on Dropbox's long-poll
//! notification endpoint ([`notify`]) rather than polling on a timer, while an
//! inotify [`watcher`] drives the upload direction. Both funnel through
//! [`reconcile`], the only component that writes [`state`].
//!
//! The design and the reasoning behind it live in `docs/architecture.md`.

pub mod api;
pub mod auth;
pub mod blocking;
pub mod config;
pub mod daemon;
pub mod error;
pub mod fsutil;
pub mod notify;
pub mod reconcile;
pub mod state;
pub mod watcher;

pub use config::Config;
pub use error::{Error, Result};
