//! The local filesystem watcher.
//!
//! Subscribes to inotify and coalesces bursts of events into one change signal
//! per path, so a single editor save does not become several uploads.
//!
//! Not yet implemented — see the
//! `implement-local-to-remote-upload-via-inotify` task in `TODO.toml`.
