//! The local filesystem watcher.
//!
//! Subscribes to inotify and coalesces bursts of events into one change signal
//! per path, so a single editor save does not become several uploads. The
//! coalescing itself lives in [`debounce`]; this file is the plumbing that
//! turns the `notify_fs` crate's synchronous callback into a stream of debounced
//! batches.
//!
//! Two things are filtered out before anything is emitted, because neither is a
//! user edit:
//!
//! - our own partial downloads (see [`crate::api::is_partial`]), and
//! - the state database, if it happens to live inside the synced folder.

mod debounce;

pub use debounce::Debouncer;

use std::path::{Path, PathBuf};
use std::time::Duration;

use notify_fs::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::error::{Error, Result};

/// A batch of local paths that have settled since the last one.
pub type LocalBatch = Vec<PathBuf>;

/// A running watcher. Dropping it unsubscribes from inotify and ends the
/// batching task, so the caller must hold it for as long as it wants events.
pub struct LocalWatcher {
    _inner: RecommendedWatcher,
}

/// Watch `root` recursively, emitting batches of paths quiet for `quiet`.
pub fn watch(root: &Path, quiet: Duration) -> Result<(LocalWatcher, mpsc::Receiver<LocalBatch>)> {
    // Unbounded, because the inotify callback is synchronous and must never
    // block the watcher thread; the debouncer downstream is what limits work.
    let (raw_tx, raw_rx) = mpsc::unbounded_channel();
    let mut inner = notify_fs::recommended_watcher(move |event| {
        // A send failure just means the consumer is gone; nothing to do.
        let _ = raw_tx.send(event);
    })
    .map_err(watch_error)?;
    inner
        .watch(root, RecursiveMode::Recursive)
        .map_err(watch_error)?;

    let (batches_tx, batches_rx) = mpsc::channel(1);
    tokio::spawn(batch(raw_rx, batches_tx, quiet));
    Ok((LocalWatcher { _inner: inner }, batches_rx))
}

type RawEvent = std::result::Result<notify_fs::Event, notify_fs::Error>;

/// Feed raw events into the debouncer and emit batches as they come due.
async fn batch(
    mut raw: mpsc::UnboundedReceiver<RawEvent>,
    batches: mpsc::Sender<LocalBatch>,
    quiet: Duration,
) {
    let mut debouncer = Debouncer::new(quiet);
    loop {
        // With nothing pending there is no deadline to wake for, so park on the
        // event stream alone rather than spinning on a timer.
        let deadline = debouncer.next_deadline();
        tokio::select! {
            event = raw.recv() => match event {
                Some(event) => note(&mut debouncer, event),
                None => return,
            },
            () = sleep_until(deadline), if deadline.is_some() => {
                let ready = debouncer.take_ready(tokio::time::Instant::now());
                if !ready.is_empty() && batches.send(ready).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        // Never resolves; the `if` guard on the select arm keeps us out of here.
        None => std::future::pending().await,
    }
}

fn note(debouncer: &mut Debouncer, event: RawEvent) {
    let now = tokio::time::Instant::now();
    match event {
        Ok(event) => {
            for path in event.paths.into_iter().filter(|path| is_interesting(path)) {
                debouncer.note(path, now);
            }
        }
        // An inotify overflow or a permission problem on one path should not
        // take the daemon down; the next full pull will catch anything missed.
        Err(error) => tracing::warn!(%error, "filesystem watch error"),
    }
}

/// Is this path a user edit we should care about?
fn is_interesting(path: &Path) -> bool {
    !crate::api::is_partial(path) && !is_state_file(path)
}

fn is_state_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("state.json"))
}

fn watch_error(error: notify_fs::Error) -> Error {
    Error::Config(format!("could not watch the local directory: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Our own downloads must never be mistaken for user edits, or the daemon
    /// would upload half-written files back to Dropbox.
    #[test]
    fn partial_downloads_are_ignored() {
        assert!(!is_interesting(Path::new("/root/a.txt.dbsync-partial")));
        assert!(is_interesting(Path::new("/root/a.txt")));
    }

    #[test]
    fn the_state_database_is_ignored() {
        assert!(!is_interesting(Path::new("/root/state.json")));
        assert!(!is_interesting(Path::new("/root/state.json.tmp")));
    }

    /// The end-to-end shape: a write inside the watched directory turns into a
    /// batch naming that file.
    #[tokio::test]
    async fn a_local_write_arrives_as_a_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (_watcher, mut batches) = watch(dir.path(), Duration::from_millis(50)).unwrap();

        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"hello").unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(10), batches.recv())
            .await
            .expect("a write should produce a batch")
            .unwrap();
        assert!(batch.iter().any(|seen| seen.ends_with("a.txt")));
    }

    /// A burst of writes to one file must not produce a batch per write.
    #[tokio::test]
    async fn a_burst_of_writes_collapses() {
        let dir = tempfile::tempdir().unwrap();
        let (_watcher, mut batches) = watch(dir.path(), Duration::from_millis(200)).unwrap();

        let path = dir.path().join("a.txt");
        for round in 0..5 {
            std::fs::write(&path, format!("write {round}")).unwrap();
        }

        let batch = tokio::time::timeout(Duration::from_secs(10), batches.recv())
            .await
            .expect("the burst should produce a batch")
            .unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[tokio::test]
    async fn watching_a_missing_directory_is_a_config_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(matches!(
            watch(&missing, Duration::from_millis(50)),
            Err(Error::Config(_))
        ));
    }
}
