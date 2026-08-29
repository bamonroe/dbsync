//! The local sync state database.
//!
//! Owns the mapping `path -> (rev, content hash, local mtime, size)` plus the
//! folder cursor, and is the only component permitted to write it. Persistence
//! must be atomic so a crash cannot leave state and disk disagreeing.
//!
//! Not yet implemented — see the `implement-the-local-sync-state-database` task
//! in `TODO.toml`. The content hashing it depends on is done.

pub mod hash;
