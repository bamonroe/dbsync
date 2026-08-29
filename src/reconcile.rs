//! The reconciler: the single point where changes are applied.
//!
//! Both directions funnel through here so a path is never written from the
//! remote and local sides at once, and so conflicts produce a
//! `filename (conflicted copy).ext` rather than destroying either version.
//!
//! Not yet implemented — see the sync tasks in `TODO.toml`.
