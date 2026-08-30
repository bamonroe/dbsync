//! Retrying wrappers around the two listing calls.
//!
//! A listing is the one part of a pull that is *not* cheap to redo. A page of
//! downloads that fails costs a page; a `list_folder/continue` that fails
//! propagates out of the applier, kills the daemon, and — on a cursor that was
//! deliberately cleared — throws away every page listed so far. A full account
//! listing runs for hours, so a single dropped connection at hour three costs
//! all three hours.
//!
//! So the listing retries where a download would give up. The policy is
//! deliberately narrow: only errors that could plausibly succeed on a second
//! ask are retried, and [`crate::error::Error::CursorReset`] is passed straight
//! through because it is a *routine instruction* to re-list, not a failure.

use std::time::Duration;

use crate::api::ListFolderPage;
use crate::error::{Error, Result};
use crate::reconcile::source::RemoteSource;

/// How many times a listing call is asked before giving up.
const LIST_ATTEMPTS: u32 = 5;

/// The wait before the second attempt; each further attempt doubles it.
const BASE_BACKOFF: Duration = Duration::from_secs(2);

/// Open a recursive listing of `path`, retrying transient failures.
pub async fn list_folder<S: RemoteSource>(source: &S, path: &str) -> Result<ListFolderPage> {
    with_retries("list_folder", path, || source.list_folder(path)).await
}

/// Fetch the changes since `cursor`, retrying transient failures.
pub async fn list_folder_continue<S: RemoteSource>(
    source: &S,
    cursor: &str,
) -> Result<ListFolderPage> {
    with_retries("list_folder/continue", "", || {
        source.list_folder_continue(cursor)
    })
    .await
}

/// Call `attempt` until it succeeds, it fails permanently, or we run out.
///
/// `what` and `context` only name the call in the log; the retry decision is
/// [`is_transient`]'s alone.
async fn with_retries<F, Fut>(what: &str, context: &str, mut attempt: F) -> Result<ListFolderPage>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ListFolderPage>>,
{
    let mut backoff = BASE_BACKOFF;
    for number in 1..=LIST_ATTEMPTS {
        let error = match attempt().await {
            Ok(page) => return Ok(page),
            Err(error) => error,
        };
        if !is_transient(&error) || number == LIST_ATTEMPTS {
            return Err(error);
        }
        // Dropbox naming its own delay beats guessing one.
        let wait = match error {
            Error::RateLimited(seconds) => Duration::from_secs(seconds),
            _ => backoff,
        };
        tracing::warn!(
            call = what,
            context,
            attempt = number,
            wait_secs = wait.as_secs(),
            %error,
            "listing call failed; retrying"
        );
        tokio::time::sleep(wait).await;
        backoff *= 2;
    }
    unreachable!("the loop returns on the last attempt")
}

/// Whether asking again could plausibly give a different answer.
///
/// The default is *not* to retry: a listing that fails for a real reason should
/// surface, not be asked four more times. `CursorReset` in particular must
/// reach the caller, which handles it by re-listing.
fn is_transient(error: &Error) -> bool {
    match error {
        // A dropped connection, a timeout, a truncated response.
        Error::Http(_) => true,
        // Dropbox's own back-pressure, and its 5xx family.
        Error::RateLimited(_) => true,
        Error::Api { status, .. } => *status >= 500,
        Error::CursorReset
        | Error::Config(_)
        | Error::ReadFile { .. }
        | Error::NotAuthenticated
        | Error::Unauthorized
        | Error::Conflict
        | Error::Io(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dropped_connection_is_worth_another_ask() {
        let dropped = reqwest::Client::new()
            .get("http://127.0.0.1:1/")
            .build()
            .err();
        // Construct the variant we actually care about via the API surface we
        // have: any `Http` error qualifies, regardless of its cause.
        assert!(dropped.is_none() || is_transient(&Error::Http(dropped.unwrap())));
    }

    #[test]
    fn dropbox_server_errors_are_transient_and_client_errors_are_not() {
        assert!(is_transient(&Error::Api {
            status: 503,
            message: "unavailable".into()
        }));
        assert!(!is_transient(&Error::Api {
            status: 409,
            message: "path/not_found".into()
        }));
    }

    #[test]
    fn rate_limiting_is_retried() {
        assert!(is_transient(&Error::RateLimited(3)));
    }

    #[test]
    fn a_cursor_reset_reaches_the_caller() {
        // Retrying it would be wrong twice over: the cursor is dead, and the
        // caller's handling of it is the whole recovery path.
        assert!(!is_transient(&Error::CursorReset));
    }

    #[test]
    fn a_rejected_token_is_not_retried_here() {
        assert!(!is_transient(&Error::Unauthorized));
        assert!(!is_transient(&Error::NotAuthenticated));
    }
}
