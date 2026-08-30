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
use crate::error::Result;
use crate::notify::{self, LongpollClient};
use crate::reconcile::{self, PathMapper, Reconciler};
use crate::state::StateDb;
use crate::watcher;

/// Run the sync daemon until a termination signal arrives.
pub async fn run(config: &Config) -> Result<Summary> {
    let mut reconciler = build(config)?;

    // Before the pull, while nothing is downloading: a partial being written
    // right now looks exactly like one abandoned by a kill.
    match reconcile::sweep::partial_downloads(&config.local_root) {
        Ok(0) => {}
        Ok(swept) => tracing::info!(swept, "removed leftover partial downloads"),
        // Scratch files left behind are not worth refusing to start over.
        Err(error) => tracing::warn!(%error, "could not sweep partial downloads"),
    }

    tracing::info!(root = %config.local_root.display(), "starting initial pull");
    let first = reconciler.pull().await?;
    tracing::info!(
        applied = first.applied,
        resynced = first.resynced,
        "initial pull complete"
    );

    let cursor = reconciler.cursor().unwrap_or_default().to_string();
    let poller = LongpollClient::new(config.longpoll.timeout_secs)?;
    let (notify_loop, handle, events) = notify::channel(poller, cursor);
    let remote = tokio::spawn(notify_loop.run());

    let quiet = Duration::from_millis(config.watcher.debounce_ms);
    // Held for the lifetime of the loop: dropping it unsubscribes from inotify.
    let (_watcher, batches) = watcher::watch(&config.local_root, quiet)?;
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
fn build(config: &Config) -> Result<Reconciler<ApiClient>> {
    // A local root that does not exist yet is normal on a first run, and the
    // watcher cannot subscribe to a missing directory.
    std::fs::create_dir_all(&config.local_root)?;

    let oauth = OauthClient::new(config.app_key.clone())?;
    let tokens = Arc::new(TokenProvider::new(oauth, TokenStore::default_location()?));
    let api = ApiClient::new(tokens)?.with_chunking(config.download.chunking());

    let paths = PathMapper::new(&config.local_root, &config.remote_root);
    let db = StateDb::default_location()?;
    let state = db.load()?;
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
