//! Termination signals.
//!
//! The daemon runs in the foreground under a supervisor (systemd, a terminal),
//! so the only shutdown it has to understand is a signal. Both signals mean the
//! same thing here: stop taking new work and let the current operation finish,
//! so the state file is never left describing a half-applied change.

use tokio::signal::unix::{SignalKind, signal};

/// Resolve when the process is asked to stop.
///
/// A signal handler that cannot be installed is not worth aborting a sync over,
/// so a failure degrades to "never fires" and the daemon keeps running until it
/// is killed outright.
pub async fn requested() {
    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(stream) => stream,
        Err(error) => never(error).await,
    };
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(error) => never(error).await,
    };
    let signal = tokio::select! {
        _ = interrupt.recv() => "SIGINT",
        _ = terminate.recv() => "SIGTERM",
    };
    tracing::info!(signal, "shutting down");
}

/// Log the reason, then park forever.
async fn never(error: std::io::Error) -> ! {
    tracing::warn!(%error, "cannot install a signal handler; shutdown must be forced");
    std::future::pending().await
}
