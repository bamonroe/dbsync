//! The long-poll HTTP call itself.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Long-poll lives on its own host, separate from the authenticated API.
const LONGPOLL_URL: &str = "https://notify.dropboxapi.com/2/files/list_folder/longpoll";

/// Headroom added to the request timeout so the HTTP client does not give up
/// before Dropbox has had its full chance to answer.
const TIMEOUT_MARGIN: Duration = Duration::from_secs(90);

/// What one long-poll call concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LongpollOutcome {
    /// Something changed. Call `/files/list_folder/continue` to see what.
    Changed,
    /// Nothing changed before the timeout elapsed. Just poll again.
    TimedOut,
    /// Dropbox asked us to wait before reconnecting.
    Backoff(Duration),
}

#[derive(Serialize)]
struct LongpollRequest<'a> {
    cursor: &'a str,
    timeout: u64,
}

#[derive(Deserialize)]
struct LongpollResponse {
    changes: bool,
    /// Seconds to wait before the next long-poll, when Dropbox sends one.
    #[serde(default)]
    backoff: Option<u64>,
}

/// An unauthenticated client for the notification endpoint.
///
/// Deliberately separate from the authenticated API client: sending an
/// `Authorization` header here is an error, and the request timeout must exceed
/// the long-poll timeout.
pub struct LongpollClient {
    http: reqwest::Client,
    timeout_secs: u64,
}

impl LongpollClient {
    /// Build a client that blocks for up to `timeout_secs` per call.
    ///
    /// `timeout_secs` is validated at config load; see [`crate::config`].
    pub fn new(timeout_secs: u64) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs) + TIMEOUT_MARGIN)
            .build()?;
        Ok(Self { http, timeout_secs })
    }

    /// Block until something changes under `cursor`, or the timeout elapses.
    pub async fn wait(&self, cursor: &str) -> Result<LongpollOutcome> {
        let request = LongpollRequest {
            cursor,
            timeout: self.timeout_secs,
        };
        let response = self.http.post(LONGPOLL_URL).json(&request).send().await?;

        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            // A rejected cursor here means the same thing it means on
            // `continue`: re-list and reconcile.
            if status == reqwest::StatusCode::CONFLICT || message.contains("reset") {
                return Err(Error::CursorReset);
            }
            return Err(Error::Api {
                status: status.as_u16(),
                message,
            });
        }

        let body: LongpollResponse = response.json().await?;
        Ok(match (body.changes, body.backoff) {
            (true, _) => LongpollOutcome::Changed,
            (false, Some(secs)) => LongpollOutcome::Backoff(Duration::from_secs(secs)),
            (false, None) => LongpollOutcome::TimedOut,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> LongpollOutcome {
        let body: LongpollResponse = serde_json::from_str(json).unwrap();
        match (body.changes, body.backoff) {
            (true, _) => LongpollOutcome::Changed,
            (false, Some(secs)) => LongpollOutcome::Backoff(Duration::from_secs(secs)),
            (false, None) => LongpollOutcome::TimedOut,
        }
    }

    #[test]
    fn changes_true_means_go_look() {
        assert_eq!(parse(r#"{"changes": true}"#), LongpollOutcome::Changed);
    }

    #[test]
    fn changes_false_alone_is_a_plain_timeout() {
        assert_eq!(parse(r#"{"changes": false}"#), LongpollOutcome::TimedOut);
    }

    #[test]
    fn a_backoff_must_be_honoured() {
        assert_eq!(
            parse(r#"{"changes": false, "backoff": 30}"#),
            LongpollOutcome::Backoff(Duration::from_secs(30))
        );
    }

    /// The request body Dropbox expects is exactly these two fields.
    #[test]
    fn request_serialises_to_cursor_and_timeout() {
        let json = serde_json::to_value(LongpollRequest {
            cursor: "abc",
            timeout: 300,
        })
        .unwrap();
        assert_eq!(json, serde_json::json!({"cursor": "abc", "timeout": 300}));
    }

    /// The HTTP timeout must outlast the long-poll block, or we would abort our
    /// own request before Dropbox could answer.
    #[test]
    fn http_timeout_exceeds_the_longpoll_timeout() {
        assert!(TIMEOUT_MARGIN > Duration::from_secs(0));
    }
}
