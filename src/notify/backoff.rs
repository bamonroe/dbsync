//! Reconnect pacing for the long-poll loop.
//!
//! Two different delays end up here and they must not be confused:
//!
//! - A **server backoff** is Dropbox telling us, in a successful response, to
//!   wait before reconnecting. It is obeyed verbatim and it does not mean
//!   anything went wrong.
//! - A **failure backoff** is ours: the connection dropped or the endpoint
//!   errored, so we retry on a capped exponential curve until it works again.

use std::time::Duration;

/// First delay after a failure.
const BASE: Duration = Duration::from_secs(1);

/// Ceiling for the exponential curve. A daemon that has been offline for an
/// hour should still notice the network coming back within a minute.
const CEILING: Duration = Duration::from_secs(60);

/// A capped exponential backoff that resets on every success.
#[derive(Debug, Clone)]
pub struct Backoff {
    attempt: u32,
}

impl Backoff {
    /// A backoff that has not failed yet.
    pub fn new() -> Self {
        Self { attempt: 0 }
    }

    /// Forget the failure history — the last attempt succeeded.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Record a failure and return how long to wait before the next attempt.
    pub fn next_delay(&mut self) -> Duration {
        let delay = BASE
            .checked_mul(1u32.checked_shl(self.attempt).unwrap_or(u32::MAX))
            .unwrap_or(CEILING)
            .min(CEILING);
        self.attempt = self.attempt.saturating_add(1);
        delay
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_failure_retries_almost_immediately() {
        assert_eq!(Backoff::new().next_delay(), BASE);
    }

    #[test]
    fn repeated_failures_double_the_wait() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(8));
    }

    /// A long outage must not push the retry interval out to hours.
    #[test]
    fn the_curve_is_capped() {
        let mut backoff = Backoff::new();
        for _ in 0..100 {
            assert!(backoff.next_delay() <= CEILING);
        }
        assert_eq!(backoff.next_delay(), CEILING);
    }

    #[test]
    fn a_success_clears_the_history() {
        let mut backoff = Backoff::new();
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), BASE);
    }
}
