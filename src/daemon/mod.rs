//! Process lifecycle: build the components from config, then run them.
//!
//! This is the only place the concrete pieces meet. Everything below it is
//! written against traits ([`RemoteSource`], [`RemoteSink`], [`Longpoll`]) so
//! the wiring is the one part that needs a real Dropbox account.
//!
//! The startup order is load-bearing:
//!
//! 0. **Sweep leftover partial downloads**, which is only safe here: once the
//!    pull starts, an in-flight partial is indistinguishable from an orphan.
//! 1. **Pull first**, before anything is watched. A first run has no cursor, so
//!    this is the full listing that produces one; a later run applies whatever
//!    changed while the daemon was down. Either way it ends with a cursor.
//! 2. **Then long-poll**, on exactly that cursor, so the window between the
//!    listing and the park is closed by the cursor rather than by luck.
//! 3. **Then watch locally**, last, because the pull writes files and there is
//!    no point in feeding our own downloads into the debouncer.
//!
//! [`Longpoll`]: crate::notify::Longpoll

mod shutdown;
mod sync;

pub use sync::Summary;

use std::sync::Arc;
use std::time::Duration;

use crate::api::ApiClient;
use crate::auth::{OauthClient, TokenProvider, TokenStore};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::notify::{self, LongpollClient};
use crate::reconcile::{self, PathMapper, Reconciler, RemoteSink, RemoteSource};
use crate::state::{FailureKind, StateDb, SyncState};
use crate::watcher;

/// Run the sync daemon until a termination signal arrives.
pub async fn run(config: &Config) -> Result<Summary> {
    let mut reconciler = build(config).await?;

    // Before the pull, while nothing is downloading: a partial being written
    // right now looks exactly like one abandoned by a kill.
    match reconcile::sweep::partial_downloads(&config.local_root).await {
        Ok(0) => {}
        Ok(swept) => tracing::info!(swept, "removed leftover partial downloads"),
        // Scratch files left behind are not worth refusing to start over.
        Err(error) => tracing::warn!(%error, "could not sweep partial downloads"),
    }

    tracing::info!(root = %config.local_root.display(), "starting initial pull");
    initial_pull(&mut reconciler).await?;
    report_failures(reconciler.state());

    let cursor = reconciler.cursor().unwrap_or_default().to_string();
    let poller = LongpollClient::new(config.longpoll.timeout_secs)?;
    let (notify_loop, handle, events) = notify::channel(poller, cursor);
    let remote = tokio::spawn(notify_loop.run());

    let quiet = Duration::from_millis(config.watcher.debounce_ms);
    // Held for the lifetime of the loop: dropping it unsubscribes from inotify.
    let (_watcher, batches) =
        watcher::watch(&config.local_root, quiet, Some(reconciler.db_path()))?;
    tracing::info!(debounce_ms = config.watcher.debounce_ms, "watching locally");

    let summary = sync::run(
        &mut reconciler,
        &handle,
        events,
        batches,
        shutdown::requested(),
    )
    .await?;

    // The loop stops at its next iteration once its events receiver is gone,
    // but it may be parked on a long-poll that runs for minutes; nothing it
    // could still do matters now, so it is aborted rather than awaited.
    remote.abort();
    tracing::info!(pulls = summary.pulls, pushes = summary.pushes, "stopped");
    Ok(summary)
}

/// Assemble the reconciler: credentials, API client, path mapping, state.
async fn build(config: &Config) -> Result<Reconciler<ApiClient>> {
    // A local root that does not exist yet is normal on a first run, and the
    // watcher cannot subscribe to a missing directory.
    tokio::fs::create_dir_all(&config.local_root).await?;

    let oauth = OauthClient::new(config.app_key.clone())?;
    let tokens = Arc::new(TokenProvider::new(oauth, TokenStore::default_location()?));
    let api = ApiClient::new(tokens)?.with_chunking(config.download.chunking());

    let paths = PathMapper::new(&config.local_root, &config.remote_root);
    let db = StateDb::default_location()?;
    let state = db.load_off_thread().await?;
    tracing::info!(
        state = %db.path().display(),
        tracked = state.len(),
        remote_root = paths.remote_root(),
        "loaded state"
    );
    let budget = config.download.budget();
    tracing::info!(
        budget_bytes = budget.bytes,
        min_concurrency = budget.floor,
        max_concurrency = budget.ceiling,
        "download budget"
    );
    Ok(Reconciler::with_budget(api, paths, db, state, budget))
}

/// Run the startup pull, surviving a failure that the next pull can resume.
///
/// The cursor is persisted page by page, so a pull that dies part-way has not
/// lost the pages it already applied: carrying on into the watch loop resumes
/// from there at the next notification, whereas exiting restarts the listing
/// from wherever the cursor stands and — on a full listing — throws hours away.
///
/// Two failures are still fatal. Without a cursor there is nothing to park a
/// long-poll on, so the daemon would idle without ever syncing; and a rejected
/// credential will reject every later call too, so retrying forever only hides
/// the one thing the operator needs to be told.
async fn initial_pull<S: RemoteSource + RemoteSink + Sync>(
    reconciler: &mut Reconciler<S>,
) -> Result<()> {
    let error = match reconciler.pull().await {
        Ok(first) => {
            tracing::info!(
                applied = first.applied,
                resynced = first.resynced,
                "initial pull complete"
            );
            return Ok(());
        }
        Err(error) => error,
    };
    if matches!(error, Error::NotAuthenticated | Error::Unauthorized) {
        return Err(error);
    }
    let Some(cursor) = reconciler.cursor() else {
        return Err(error);
    };
    tracing::warn!(
        %error,
        cursor_len = cursor.len(),
        "initial pull did not finish; resuming from the saved cursor on the next change"
    );
    Ok(())
}

/// Say out loud what is missing, so a failed file is never silently absent.
///
/// A warning rather than info, because "the pull finished" and "everything
/// arrived" are different claims and only the second one is what an operator
/// assumes. Silent when there is nothing missing.
fn report_failures(state: &SyncState) {
    let total = state.failure_count();
    if total == 0 {
        return;
    }
    let permanent = state
        .failures()
        .filter(|f| f.kind == FailureKind::Permanent)
        .count();
    tracing::warn!(
        failed = total,
        permanent,
        retryable = total - permanent,
        "some entries are not in sync; run `dbsync failures` to list them"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::reconcile::testing::FakeRemote;
    use crate::state::SyncState;

    /// A reconciler over a throwaway directory, holding `cursor` as its
    /// position, whose next listing call fails with `error`.
    fn failing(cursor: Option<&str>, error: Error) -> (tempfile::TempDir, Reconciler<FakeRemote>) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let mut state = SyncState::new();
        if let Some(cursor) = cursor {
            state.set_cursor(cursor);
        }
        let remote = FakeRemote::new();
        // A cursor sends the pull down `continue`; without one it re-lists.
        if cursor.is_some() {
            remote.queue_continue(Err(error));
        } else {
            remote.queue_listing(Err(error));
        }
        let reconciler = Reconciler::new(
            remote,
            PathMapper::new(&root, ""),
            StateDb::at(dir.path().join("state.json")),
            state,
        );
        (dir, reconciler)
    }

    #[tokio::test]
    async fn a_failed_pull_is_survivable_once_there_is_a_cursor_to_resume_from() {
        let (_dir, mut reconciler) = failing(
            Some("c1"),
            Error::Api {
                status: 400,
                message: "boom".into(),
            },
        );
        // The daemon carries on: the pages already applied stand, and the next
        // notification resumes from the saved cursor.
        assert!(initial_pull(&mut reconciler).await.is_ok());
    }

    #[tokio::test]
    async fn a_failed_first_listing_is_fatal_because_there_is_no_cursor_to_park_on() {
        let (_dir, mut reconciler) = failing(
            None,
            Error::Api {
                status: 400,
                message: "boom".into(),
            },
        );
        assert!(initial_pull(&mut reconciler).await.is_err());
    }

    #[tokio::test]
    async fn a_rejected_credential_is_fatal_even_with_a_cursor() {
        // Every later call would be rejected too; looping would hide the one
        // thing the operator has to act on.
        let (_dir, mut reconciler) = failing(Some("c1"), Error::Unauthorized);
        assert!(initial_pull(&mut reconciler).await.is_err());
    }
}
