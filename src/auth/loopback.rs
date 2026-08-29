//! A one-shot loopback listener that catches the OAuth redirect.
//!
//! Dropbox redirects the browser to `http://localhost:53682/?code=…` after the
//! user approves. This is the smallest thing that can receive that: it binds
//! loopback only, accepts connections until one carries a code, answers with a
//! short page telling the user to close the tab, and stops.
//!
//! It is not a web server — it never leaves the loopback interface, and the
//! project deliberately has no publicly reachable component. See
//! `docs/architecture.md`.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::error::{Error, Result};

/// How long to wait for the user to finish approving in the browser.
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(300);

/// Cap on the request bytes read, so a stray client cannot exhaust memory.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// What the redirect carried back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redirect {
    pub code: String,
    pub state: String,
}

/// Bind loopback and wait for the redirect carrying an authorization code.
pub async fn wait_for_redirect(port: u16) -> Result<Redirect> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let accept = async {
        loop {
            let (stream, _) = listener.accept().await?;
            if let Some(redirect) = handle_connection(stream).await? {
                return Ok(redirect);
            }
            // Browsers open speculative connections and ask for /favicon.ico;
            // ignore anything that is not the redirect and keep listening.
        }
    };

    tokio::time::timeout(APPROVAL_TIMEOUT, accept)
        .await
        .map_err(|_| Error::Config("timed out waiting for browser approval".into()))?
}

/// Read one request and answer it. Returns `None` if it was not the redirect.
async fn handle_connection(mut stream: TcpStream) -> Result<Option<Redirect>> {
    let mut buf = vec![0u8; MAX_REQUEST_BYTES];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);

    let target = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("");

    match parse_redirect(target) {
        Some(redirect) => {
            respond(
                &mut stream,
                "200 OK",
                "dbsync is linked. You can close this tab.",
            )
            .await?;
            Ok(Some(redirect))
        }
        None if is_error_redirect(target) => {
            respond(&mut stream, "200 OK", "Authorization was denied.").await?;
            Err(Error::Config(
                "authorization was denied in the browser".into(),
            ))
        }
        None => {
            respond(
                &mut stream,
                "404 Not Found",
                "Waiting for the Dropbox redirect.",
            )
            .await?;
            Ok(None)
        }
    }
}

async fn respond(stream: &mut TcpStream, status: &str, message: &str) -> Result<()> {
    let body = format!("<!doctype html><meta charset=utf-8><p>{message}</p>");
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Pull `code` and `state` out of a request target such as
/// `/?code=abc&state=xyz`. Returns `None` if either is missing.
fn parse_redirect(target: &str) -> Option<Redirect> {
    let query = target.split_once('?')?.1;
    let mut code = None;
    let mut state = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=')?;
        let value = urlencoding::decode(value).ok()?.into_owned();
        match key {
            "code" => code = Some(value),
            "state" => state = Some(value),
            _ => {}
        }
    }
    Some(Redirect {
        code: code?,
        state: state?,
    })
}

/// True when Dropbox redirected with an `error` instead of a code.
fn is_error_redirect(target: &str) -> bool {
    target
        .split_once('?')
        .is_some_and(|(_, query)| query.split('&').any(|p| p.starts_with("error=")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_code_and_state() {
        assert_eq!(
            parse_redirect("/?code=abc&state=xyz"),
            Some(Redirect {
                code: "abc".into(),
                state: "xyz".into()
            })
        );
    }

    #[test]
    fn parameter_order_does_not_matter() {
        assert_eq!(
            parse_redirect("/?state=xyz&code=abc"),
            Some(Redirect {
                code: "abc".into(),
                state: "xyz".into()
            })
        );
    }

    #[test]
    fn percent_encoded_values_are_decoded() {
        let parsed = parse_redirect("/?code=a%2Bb%2Fc&state=s").unwrap();
        assert_eq!(parsed.code, "a+b/c");
    }

    #[test]
    fn extra_parameters_are_ignored() {
        assert!(parse_redirect("/?code=abc&state=xyz&scope=files").is_some());
    }

    /// A browser's speculative request or favicon fetch must not be mistaken
    /// for the redirect.
    #[test]
    fn a_request_without_a_query_is_not_the_redirect() {
        assert_eq!(parse_redirect("/favicon.ico"), None);
        assert_eq!(parse_redirect("/"), None);
    }

    #[test]
    fn a_redirect_missing_the_state_is_rejected() {
        assert_eq!(parse_redirect("/?code=abc"), None);
    }

    #[test]
    fn a_denial_is_recognised_as_an_error_redirect() {
        assert!(is_error_redirect("/?error=access_denied"));
        assert!(!is_error_redirect("/?code=abc&state=x"));
        assert!(!is_error_redirect("/favicon.ico"));
    }
}
