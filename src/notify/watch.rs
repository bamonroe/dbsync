//! The driving loop: park on the long-poll endpoint forever and emit signals.
//!
//! The loop itself is deliberately dumb. It knows how to hold a cursor, how to
//! wait, and how to keep the connection alive across errors — it does **not**
//! know what changed. Learning that is `/files/list_folder/continue`, which is
//! the reconciler's job, so the only thing crossing this boundary is a nudge.

use std::future::Future;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::backoff::Backoff;
use super::longpoll::{LongpollClient, LongpollOutcome};
use crate::error::{Error, Result};

/// What the loop tells the reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteEvent {
    /// Something changed under the cursor. Call `continue` to find out what.
    Changed,
    /// Dropbox invalidated the cursor. Re-list the folder, reconcile from
    /// scratch, and publish a fresh cursor with the [`CursorHandle`].
    CursorReset,
}

/// The long-poll call, abstracted so the loop can be tested without a network.
pub trait Longpoll {
    /// Block until something changes under `cursor`, or the timeout elapses.
    fn wait(&self, cursor: &str) -> impl Future<Output = Result<LongpollOutcome>> + Send;
}

impl Longpoll for LongpollClient {
    fn wait(&self, cursor: &str) -> impl Future<Output = Result<LongpollOutcome>> + Send {
        LongpollClient::wait(self, cursor)
    }
}

/// The reconciler's end of the cursor: the only way to hand the loop a new one.
///
/// A cursor is replaced on every `continue` and after every reset, so it is a
/// `watch` channel rather than a plain value — the loop always polls the
/// latest, and a park on a stale cursor is woken by the update.
pub struct CursorHandle(watch::Sender<String>);

impl CursorHandle {
    /// Publish the cursor the next long-poll should use.
    pub fn publish(&self, cursor: impl Into<String>) {
        // A closed receiver just means the loop has stopped; not an error.
        let _ = self.0.send(cursor.into());
    }
}

/// Wire up a loop over `poller`, starting from `cursor`.
///
/// Returns the loop, the handle used to replace its cursor, and the receiver
/// the reconciler reads change signals from. Dropping the receiver stops the
/// loop at its next iteration.
pub fn channel<P: Longpoll>(
    poller: P,
    cursor: impl Into<String>,
) -> (NotifyLoop<P>, CursorHandle, mpsc::Receiver<RemoteEvent>) {
    let (cursor_tx, cursor_rx) = watch::channel(cursor.into());
    // Depth 1: a queued nudge and a new nudge mean the same thing, so there is
    // nothing to gain from buffering more of them.
    let (events_tx, events_rx) = mpsc::channel(1);
    let notify_loop = NotifyLoop {
        poller,
        cursor: cursor_rx,
        events: events_tx,
        backoff: Backoff::new(),
    };
    (notify_loop, CursorHandle(cursor_tx), events_rx)
}

/// The long-poll driving loop.
pub struct NotifyLoop<P> {
    poller: P,
    cursor: watch::Receiver<String>,
    events: mpsc::Sender<RemoteEvent>,
    backoff: Backoff,
}

/// What the loop decided to do after one iteration.
enum Step {
    /// Poll again after this delay (which may be zero).
    Again(Duration),
    /// The reconciler is gone; wind down.
    Stop,
}

impl<P: Longpoll> NotifyLoop<P> {
    /// Run until the event receiver is dropped.
    pub async fn run(mut self) {
        loop {
            let cursor = self.cursor.borrow().clone();
            let step = match self.poller.wait(&cursor).await {
                Ok(outcome) => self.on_outcome(outcome).await,
                Err(error) => self.on_error(error).await,
            };
            match step {
                Step::Stop => return,
                Step::Again(delay) if delay.is_zero() => continue,
                Step::Again(delay) => tokio::time::sleep(delay).await,
            }
        }
    }

    /// A successful call: the connection is healthy, so the failure curve resets.
    async fn on_outcome(&mut self, outcome: LongpollOutcome) -> Step {
        self.backoff.reset();
        match outcome {
            LongpollOutcome::Changed => match self.emit(RemoteEvent::Changed).await {
                true => Step::Again(Duration::ZERO),
                false => Step::Stop,
            },
            LongpollOutcome::TimedOut => Step::Again(Duration::ZERO),
            // Dropbox asked for this pause explicitly; obey it verbatim.
            LongpollOutcome::Backoff(delay) => Step::Again(delay),
        }
    }

    /// A failed call. A reset is routine and handled; anything else is retried.
    async fn on_error(&mut self, error: Error) -> Step {
        match error {
            Error::CursorReset => {
                self.backoff.reset();
                if !self.emit(RemoteEvent::CursorReset).await {
                    return Step::Stop;
                }
                // Polling the dead cursor again would just fail the same way,
                // so idle until the reconciler publishes a live one.
                self.cursor.mark_unchanged();
                match self.cursor.changed().await {
                    Ok(()) => Step::Again(Duration::ZERO),
                    Err(_) => Step::Stop,
                }
            }
            Error::RateLimited(secs) => {
                self.backoff.reset();
                Step::Again(Duration::from_secs(secs))
            }
            other => {
                let delay = self.backoff.next_delay();
                tracing::warn!(error = %other, retry_in = ?delay, "long-poll failed");
                Step::Again(delay)
            }
        }
    }

    /// Send a signal; `false` means the reconciler has gone away.
    async fn emit(&self, event: RemoteEvent) -> bool {
        self.events.send(event).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A poller that replays a canned script of outcomes, then blocks forever
    /// so the loop parks instead of spinning off the end of the script.
    ///
    /// Each call must first take a permit from `gate`. That makes the tests
    /// deterministic: without it, the loop races ahead to its next poll the
    /// instant a signal is delivered, and "how many polls have happened by
    /// now" is a coin flip.
    struct ScriptedPoller {
        script: Mutex<std::vec::IntoIter<Result<LongpollOutcome>>>,
        seen: Mutex<Vec<String>>,
        gate: tokio::sync::Semaphore,
    }

    impl ScriptedPoller {
        /// A poller gated to exactly `script.len()` calls — the loop's poll
        /// after the last scripted one blocks instead of running ahead.
        fn new(script: Vec<Result<LongpollOutcome>>) -> Arc<Self> {
            let permits = script.len();
            let poller = Self::gated(script);
            poller.allow(permits);
            poller
        }

        /// A poller that will not answer until `allow` says so.
        fn gated(script: Vec<Result<LongpollOutcome>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script.into_iter()),
                seen: Mutex::new(Vec::new()),
                gate: tokio::sync::Semaphore::new(0),
            })
        }

        /// Let `n` more polls through.
        fn allow(&self, n: usize) {
            self.gate.add_permits(n);
        }

        fn cursors(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl Longpoll for Arc<ScriptedPoller> {
        async fn wait(&self, cursor: &str) -> Result<LongpollOutcome> {
            self.gate.acquire().await.unwrap().forget();
            self.seen.lock().unwrap().push(cursor.to_string());
            let next = self.script.lock().unwrap().next();
            match next {
                Some(outcome) => outcome,
                None => std::future::pending().await,
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_change_becomes_a_signal() {
        let poller = ScriptedPoller::new(vec![Ok(LongpollOutcome::Changed)]);
        let (loop_, _cursor, mut events) = channel(poller.clone(), "c0");
        tokio::spawn(loop_.run());

        assert_eq!(events.recv().await, Some(RemoteEvent::Changed));
    }

    /// A timeout is not an event: the loop reconnects silently.
    #[tokio::test(start_paused = true)]
    async fn a_timeout_reconnects_without_signalling() {
        let poller = ScriptedPoller::new(vec![
            Ok(LongpollOutcome::TimedOut),
            Ok(LongpollOutcome::TimedOut),
            Ok(LongpollOutcome::Changed),
        ]);
        let (loop_, _cursor, mut events) = channel(poller.clone(), "c0");
        tokio::spawn(loop_.run());

        assert_eq!(events.recv().await, Some(RemoteEvent::Changed));
        assert_eq!(poller.cursors().len(), 3);
    }

    /// Every poll reads the cursor afresh, so a `continue` mid-flight is picked
    /// up by the very next call.
    #[tokio::test(start_paused = true)]
    async fn a_published_cursor_is_used_by_the_next_poll() {
        let poller = ScriptedPoller::gated((0..3).map(|_| Ok(LongpollOutcome::Changed)).collect());
        let (loop_, cursor, mut events) = channel(poller.clone(), "c0");
        tokio::spawn(loop_.run());

        poller.allow(1);
        events.recv().await.unwrap();
        cursor.publish("c1");
        // Two more polls: the loop may already have snapshotted `c0` for the
        // one it was entering, but the call after that must read `c1`.
        poller.allow(2);
        events.recv().await.unwrap();
        events.recv().await.unwrap();

        let seen = poller.cursors();
        assert_eq!(seen[0], "c0");
        assert_eq!(seen.last().unwrap(), "c1");
    }

    /// A reset is reported once, and the loop then idles rather than hammering
    /// the endpoint with a cursor Dropbox has already rejected.
    #[tokio::test(start_paused = true)]
    async fn a_cursor_reset_is_reported_then_the_loop_waits() {
        let poller =
            ScriptedPoller::new(vec![Err(Error::CursorReset), Ok(LongpollOutcome::Changed)]);
        let (loop_, cursor, mut events) = channel(poller.clone(), "stale");
        tokio::spawn(loop_.run());

        assert_eq!(events.recv().await, Some(RemoteEvent::CursorReset));
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(poller.cursors(), vec!["stale".to_string()]);

        cursor.publish("fresh");
        assert_eq!(events.recv().await, Some(RemoteEvent::Changed));
        assert_eq!(poller.cursors()[1], "fresh");
    }

    /// A transport failure must not kill the daemon — it retries on the curve.
    #[tokio::test(start_paused = true)]
    async fn a_transient_error_is_retried() {
        let poller = ScriptedPoller::new(vec![
            Err(Error::Api {
                status: 503,
                message: "unavailable".into(),
            }),
            Ok(LongpollOutcome::Changed),
        ]);
        let (loop_, _cursor, mut events) = channel(poller.clone(), "c0");
        tokio::spawn(loop_.run());

        assert_eq!(events.recv().await, Some(RemoteEvent::Changed));
    }

    /// Dropping the receiver is how the daemon shuts the loop down.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_receiver_stops_the_loop() {
        let poller = ScriptedPoller::new(vec![
            Ok(LongpollOutcome::Changed),
            Ok(LongpollOutcome::Changed),
        ]);
        let (loop_, _cursor, events) = channel(poller.clone(), "c0");
        let handle = tokio::spawn(loop_.run());
        drop(events);

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("loop should stop once nobody is listening")
            .unwrap();
    }
}
