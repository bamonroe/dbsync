//! The authenticated HTTP client and its error mapping.
//!
//! Everything that needs an `Authorization` header goes through here, so there
//! is exactly one place that knows how to turn a Dropbox HTTP failure into a
//! [`crate::Error`] and one place that retries a call after refreshing an
//! expired token.

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::chunks::Chunking;
use super::range::ByteRange;
use crate::auth::TokenProvider;
use crate::error::{Error, Result};

/// RPC endpoints (metadata, listings) live here.
pub const RPC_HOST: &str = "https://api.dropboxapi.com/2";

/// Content endpoints (upload, download) live on a separate host.
pub const CONTENT_HOST: &str = "https://content.dropboxapi.com/2";

/// The authenticated Dropbox client.
#[derive(Clone)]
pub struct ApiClient {
    http: reqwest::Client,
    tokens: Arc<TokenProvider>,
    /// How a large file is split for download. Carried here rather than read
    /// per call: it is configuration, fixed for the life of the daemon.
    chunking: Chunking,
}

impl ApiClient {
    pub fn new(tokens: Arc<TokenProvider>) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder().build()?,
            tokens,
            chunking: Chunking::default(),
        })
    }

    /// Use `chunking` instead of the built-in defaults.
    pub fn with_chunking(mut self, chunking: Chunking) -> Self {
        self.chunking = chunking;
        self
    }

    /// The chunk limits in force.
    pub(super) fn chunking(&self) -> Chunking {
        self.chunking
    }

    /// POST a JSON body to an RPC endpoint and decode the JSON reply.
    ///
    /// Retries once on a rejected token, since the usual cause is an access
    /// token that expired between the cache check and the request.
    pub(super) async fn rpc<Req, Res>(&self, endpoint: &str, body: &Req) -> Result<Res>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let url = format!("{RPC_HOST}/{endpoint}");
        let response = match self
            .send_rpc(&url, body, self.tokens.access_token().await?)
            .await
        {
            Err(Error::Unauthorized) => {
                let token = self.tokens.force_refresh().await?;
                self.send_rpc(&url, body, token).await?
            }
            other => other?,
        };
        Ok(response.json().await?)
    }

    async fn send_rpc<Req: Serialize>(
        &self,
        url: &str,
        body: &Req,
        token: String,
    ) -> Result<reqwest::Response> {
        let response = self
            .http
            .post(url)
            .bearer_auth(token)
            .json(body)
            .send()
            .await?;
        check(response).await
    }

    /// A GET-style content request: the argument travels in a header, and the
    /// body is the file. Returns the streaming response for the caller to drain.
    /// `range` says which bytes to ask for: the whole file, everything from an
    /// offset onwards (a resume), or one bounded chunk. A bounded range is
    /// verified against the reply, so a server that ignores it is an error
    /// rather than a whole file written into one chunk's slot.
    pub(super) async fn content_download_from<Arg: Serialize>(
        &self,
        endpoint: &str,
        arg: &Arg,
        range: ByteRange,
    ) -> Result<reqwest::Response> {
        let url = format!("{CONTENT_HOST}/{endpoint}");
        let arg = serde_json::to_string(arg).map_err(|error| Error::Config(error.to_string()))?;
        let response = match self
            .send_download(&url, &arg, self.tokens.access_token().await?, range)
            .await
        {
            Err(Error::Unauthorized) => {
                let token = self.tokens.force_refresh().await?;
                self.send_download(&url, &arg, token, range).await
            }
            other => other,
        }?;
        range.verify(&response)?;
        Ok(response)
    }

    /// A content request that carries bytes up: the argument travels in a
    /// header and the body is the file content.
    ///
    /// Returns the raw response: some session endpoints answer with a file
    /// metadata document and others with an empty body.
    pub(super) async fn content_upload<Arg: Serialize>(
        &self,
        endpoint: &str,
        arg: &Arg,
        body: Vec<u8>,
    ) -> Result<reqwest::Response> {
        let url = format!("{CONTENT_HOST}/{endpoint}");
        let arg = serde_json::to_string(arg).map_err(|error| Error::Config(error.to_string()))?;
        let token = self.tokens.access_token().await?;
        match self.send_upload(&url, &arg, body.clone(), token).await {
            Err(Error::Unauthorized) => {
                let token = self.tokens.force_refresh().await?;
                self.send_upload(&url, &arg, body, token).await
            }
            other => other,
        }
    }

    async fn send_upload(
        &self,
        url: &str,
        arg: &str,
        body: Vec<u8>,
        token: String,
    ) -> Result<reqwest::Response> {
        let response = self
            .http
            .post(url)
            .bearer_auth(token)
            .header("Dropbox-API-Arg", arg)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await?;
        check(response).await
    }

    async fn send_download(
        &self,
        url: &str,
        arg: &str,
        token: String,
        range: ByteRange,
    ) -> Result<reqwest::Response> {
        let mut request = self
            .http
            .post(url)
            .bearer_auth(token)
            .header("Dropbox-API-Arg", arg);
        // Asking for the whole file with a header would work, but omitting it
        // keeps the common request byte-identical to what it always was.
        if !range.is_whole_file() {
            request = request.header(reqwest::header::RANGE, range.header_value());
        }
        check(request.send().await?).await
    }
}

/// Turn a non-success response into the error the caller should act on.
///
/// The distinctions matter: `Unauthorized` triggers a refresh-and-retry,
/// `CursorReset` triggers a full re-list, and `RateLimited` carries the delay
/// Dropbox asked for. Everything else is opaque.
async fn check(response: reqwest::Response) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(Error::Unauthorized);
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(Error::RateLimited(retry_after(&response)));
    }
    let message = response.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::CONFLICT {
        // Both are 409s and only the body tells them apart: a dead cursor is
        // routine bookkeeping, a rejected write is a conflict to preserve.
        if message.contains("reset") {
            return Err(Error::CursorReset);
        }
        if message.contains("conflict") {
            return Err(Error::Conflict);
        }
    }
    Err(Error::Api {
        status: status.as_u16(),
        message,
    })
}

/// Default pause when Dropbox rate-limits us without saying for how long.
const DEFAULT_RETRY_AFTER: u64 = 30;

fn retry_after(response: &reqwest::Response) -> u64 {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_RETRY_AFTER)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two hosts are genuinely different; sending a download to the RPC
    /// host is an error Dropbox reports confusingly, so pin them.
    #[test]
    fn rpc_and_content_are_separate_hosts() {
        assert_ne!(RPC_HOST, CONTENT_HOST);
        assert!(RPC_HOST.starts_with("https://api."));
        assert!(CONTENT_HOST.starts_with("https://content."));
    }

    fn response(status: u16, body: &str) -> reqwest::Response {
        reqwest::Response::from(
            http::Response::builder()
                .status(status)
                .body(body.to_string())
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn a_401_asks_the_caller_to_refresh() {
        assert!(matches!(
            check(response(401, "expired")).await,
            Err(Error::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn a_reset_cursor_is_not_a_generic_api_error() {
        assert!(matches!(
            check(response(409, r#"{"error": {".tag": "reset"}}"#)).await,
            Err(Error::CursorReset)
        ));
    }

    /// A refused write is the conflict path, not a generic failure: the caller
    /// must keep both versions rather than log and move on.
    #[tokio::test]
    async fn a_refused_write_is_a_conflict() {
        assert!(matches!(
            check(response(
                409,
                r#"{"error": {".tag": "path", "reason": {".tag": "conflict"}}}"#
            ))
            .await,
            Err(Error::Conflict)
        ));
    }

    /// A 409 that isn't a reset — a missing path, say — must stay an API error,
    /// or every such failure would trigger a pointless full re-list.
    #[tokio::test]
    async fn an_unrelated_409_stays_an_api_error() {
        assert!(matches!(
            check(response(409, r#"{"error": {".tag": "path_not_found"}}"#)).await,
            Err(Error::Api { status: 409, .. })
        ));
    }

    #[tokio::test]
    async fn a_429_without_a_header_still_produces_a_delay() {
        assert!(matches!(
            check(response(429, "slow down")).await,
            Err(Error::RateLimited(DEFAULT_RETRY_AFTER))
        ));
    }

    #[tokio::test]
    async fn a_success_passes_through() {
        assert!(check(response(200, "{}")).await.is_ok());
    }
}
