//! The sync loop: one task, both directions.
//!
//! Downloads and uploads share a single [`Reconciler`], which owns the state
//! database, so they cannot run concurrently — this loop is what serialises
//! them. A remote pull and a local push never overlap, which is what keeps a
//! file from being written from both ends at once.
//!
//! The loop owns no I/O of its own: it reads a remote nudge channel
//! ([`crate::notify`]), a local batch channel ([`crate::watcher`]), and a
//! shutdown future, and does nothing else. That is what makes it testable
//! without a network or a filesystem watcher.

use std::future::Future;
use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::error::Result;
use crate::notify::{CursorHandle, RemoteEvent};
use crate::reconcile::{Reconciler, RemoteSink, RemoteSource};

/// What one run of the loop did, for logging and for the tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Summary {
    /// How many remote nudges were answered with a pull.
    pub pulls: usize,
    /// How many local batches were pushed.
    pub pushes: usize,
}

/// Run until shutdown is requested or either channel closes.
///
/// Every pull republishes the cursor through `cursor`, because the long-poll
/// loop is parked on the previous one and would otherwise be told about the
/// same change again on its next wake.
pub async fn run<S: RemoteSource + RemoteSink>(
    reconciler: &mut Reconciler<S>,
    cursor: &CursorHandle,
    mut events: mpsc::Receiver<RemoteEvent>,
    mut batches: mpsc::Receiver<Vec<PathBuf>>,
    shutdown: impl Future<Output = ()>,
) -> Result<Summary> {
    let mut summary = Summary::default();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            // Biased so a pending shutdown wins over a busy channel; without
            // it, a directory changing constantly could starve the exit.
            biased;
            () = &mut shutdown => return Ok(summary),
            event = events.recv() => match event {
                Some(event) => {
                    pull(reconciler, cursor, event).await;
                    summary.pulls += 1;
                }
                // The long-poll loop only stops when it is dropped, so this
                // means the remote half is gone for good.
                None => return Ok(summary),
            },
            batch = batches.recv() => match batch {
                Some(batch) => {
                    push(reconciler, &batch).await;
                    summary.pushes += 1;
                }
                None => return Ok(summary),
            },
        }
    }
}

/// Answer one remote nudge, then hand the new cursor back to the long-poll loop.
///
/// A failed pull is logged rather than fatal: the cursor was not advanced past
/// anything unapplied, so the next notification retries the same work.
async fn pull<S: RemoteSource + RemoteSink>(
    reconciler: &mut Reconciler<S>,
    cursor: &CursorHandle,
    event: RemoteEvent,
) {
    if event == RemoteEvent::CursorReset {
        tracing::info!("cursor reset; rebuilding from a full listing");
    }
    match reconciler.pull().await {
        Ok(pull) => tracing::info!(
            applied = pull.applied,
            resynced = pull.resynced,
            "applied remote changes"
        ),
        Err(error) => tracing::warn!(%error, "pull failed; will retry on the next notification"),
    }
    // Published even after a failure: a partial pull still advanced the cursor
    // page by page, and the loop must not park on the one we started from.
    if let Some(fresh) = reconciler.cursor() {
        cursor.publish(fresh);
    }
}

/// Upload one settled batch of local paths.
async fn push<S: RemoteSource + RemoteSink>(reconciler: &mut Reconciler<S>, batch: &[PathBuf]) {
    match reconciler.push(batch).await {
        Ok(push) => tracing::info!(
            uploaded = push.uploaded,
            deleted = push.deleted,
            conflicted = push.conflicted,
            "pushed local changes"
        ),
        Err(error) => tracing::warn!(%error, "push failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use crate::api::{ListFolderPage, RemoteEntry, RemoteFile};
    use crate::error::Result as CrateResult;
    use crate::notify::{Longpoll, LongpollOutcome};
    use crate::reconcile::PathMapper;
    use crate::reconcile::testing::FakeRemote;
    use crate::state::{StateDb, SyncState};

    /// A poller that never answers: the tests drive the event channel by hand,
    /// so the loop only exists to hand back a [`CursorHandle`].
    struct IdlePoller;

    impl Longpoll for IdlePoller {
        async fn wait(&self, _cursor: &str) -> CrateResult<LongpollOutcome> {
            std::future::pending().await
        }
    }

    /// A cursor handle detached from any running long-poll loop.
    fn cursor_handle() -> CursorHandle {
        let (_loop, handle, _events) = crate::notify::channel(IdlePoller, "cursor-1");
        handle
    }

    fn page(cursor: &str, entries: Vec<RemoteEntry>) -> ListFolderPage {
        ListFolderPage {
            entries,
            cursor: cursor.to_string(),
            has_more: false,
        }
    }

    fn file(path: &str, rev: &str) -> RemoteEntry {
        RemoteEntry::File(RemoteFile {
            path_lower: path.to_lowercase(),
            path_display: path.to_string(),
            rev: rev.to_string(),
            size: 0,
            content_hash: None,
        })
    }

    fn reconciler(remote: FakeRemote, dir: &std::path::Path) -> Reconciler<FakeRemote> {
        Reconciler::new(
            remote,
            PathMapper::new(dir, ""),
            StateDb::at(dir.join("state.json")),
            SyncState::new(),
        )
    }

    /// A remote nudge is answered with a pull, and the cursor it produced is
    /// republished so the long-poll loop stops asking about the same change.
    #[tokio::test]
    async fn a_remote_event_is_answered_with_a_pull() {
        let dir = tempfile::tempdir().unwrap();
        let remote = FakeRemote::new();
        remote.put("/a.txt", b"hello");
        remote.queue_listing(Ok(page("cursor-2", vec![file("/a.txt", "r1")])));
        let mut reconciler = reconciler(remote, dir.path());

        let (events_tx, events) = mpsc::channel(1);
        let (_batches_tx, batches) = mpsc::channel::<Vec<PathBuf>>(1);
        events_tx.send(RemoteEvent::Changed).await.unwrap();
        drop(events_tx);

        let summary = run(
            &mut reconciler,
            &cursor_handle(),
            events,
            batches,
            std::future::pending(),
        )
        .await
        .unwrap();

        assert_eq!(summary.pulls, 1);
        assert_eq!(reconciler.cursor(), Some("cursor-2"));
        assert_eq!(std::fs::read(dir.path().join("a.txt")).unwrap(), b"hello");
    }

    /// A settled local batch becomes an upload.
    #[tokio::test]
    async fn a_local_batch_is_pushed() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("note.txt");
        std::fs::write(&local, b"typed").unwrap();
        let mut reconciler = reconciler(FakeRemote::new(), dir.path());

        let (_events_tx, events) = mpsc::channel::<RemoteEvent>(1);
        let (batches_tx, batches) = mpsc::channel(1);
        batches_tx.send(vec![local]).await.unwrap();
        drop(batches_tx);

        let summary = run(
            &mut reconciler,
            &cursor_handle(),
            events,
            batches,
            std::future::pending(),
        )
        .await
        .unwrap();

        assert_eq!(summary.pushes, 1);
    }

    /// Shutdown wins over pending work, so a busy directory cannot keep the
    /// daemon alive past a signal.
    #[tokio::test(start_paused = true)]
    async fn shutdown_ends_the_loop() {
        let dir = tempfile::tempdir().unwrap();
        let mut reconciler = reconciler(FakeRemote::new(), dir.path());
        let (_events_tx, events) = mpsc::channel::<RemoteEvent>(1);
        let (_batches_tx, batches) = mpsc::channel::<Vec<PathBuf>>(1);

        let summary = run(
            &mut reconciler,
            &cursor_handle(),
            events,
            batches,
            tokio::time::sleep(Duration::from_secs(1)),
        )
        .await
        .unwrap();

        assert_eq!(summary, Summary::default());
    }
}
