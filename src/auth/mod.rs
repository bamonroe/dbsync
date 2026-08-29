//! OAuth2 PKCE login and token refresh.
//!
//! dbsync stores no app secret. It is a public OAuth client: the PKCE flow
//! (RFC 7636) proves possession of the verifier instead, so only the app key
//! ships in `config.toml`. The long-lived refresh token is kept outside the
//! repo with owner-only permissions; see [`store`].
//!
//! There are two login flows, differing only in how the authorization code
//! gets back here.
//!
//! [`login`] catches a browser redirect:
//!
//! 1. Generate a PKCE verifier/challenge and a random `state`.
//! 2. Print the authorize URL for the user to open.
//! 3. Catch the redirect on loopback ([`loopback`]) and check `state` matches.
//! 4. Exchange the code plus the verifier for tokens ([`oauth`]).
//! 5. Persist the refresh token.
//!
//! [`login_with_pasted_code`] sends no `redirect_uri`, so Dropbox displays the
//! code and the user pastes it at a prompt — steps 3 and its `state` check drop
//! out. That is the only flow that works on a headless host, where the browser
//! is on another machine and cannot reach a loopback listener here.
//!
//! Thereafter [`TokenProvider`] supplies access tokens and refreshes them.

mod loopback;
mod oauth;
mod pkce;
mod provider;
mod store;

use std::io::{self, Write};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

pub use loopback::{Redirect, wait_for_redirect as wait_for_redirect_on};
pub use oauth::{OauthClient, redirect_uri};
pub use provider::TokenProvider;
pub use store::{StoredCredentials, TokenStore};

use crate::error::{Error, Result};

/// Bytes of entropy in the CSRF `state` parameter.
const STATE_BYTES: usize = 16;

/// Run the interactive login flow and persist the resulting refresh token.
///
/// Prints the authorize URL rather than launching a browser: the daemon
/// normally runs in a container, where there is none.
pub async fn login(app_key: &str, store: &TokenStore) -> Result<StoredCredentials> {
    let pkce = pkce::Pkce::generate();
    let state = random_state();
    let url = oauth::authorize_url(app_key, &pkce.challenge, &state);

    println!("Open this URL to authorize dbsync:\n\n  {url}\n");
    println!("Waiting for the redirect on {} ...", redirect_uri());

    let redirect = loopback::wait_for_redirect(oauth::REDIRECT_PORT).await?;

    // Guards against a third party tricking the loopback listener into
    // exchanging a code that this flow did not initiate.
    if redirect.state != state {
        return Err(Error::Config(
            "state parameter did not match — aborting the login".into(),
        ));
    }

    let client = OauthClient::new(app_key.to_string())?;
    let tokens = client.exchange_code(&redirect.code, &pkce.verifier).await?;
    persist(tokens, store)
}

/// Run the login flow without a redirect, reading the code from stdin.
///
/// For headless hosts: the user approves the app in a browser on a *different*
/// machine, Dropbox shows them a code, and they paste it here. Nothing has to
/// listen on a port, so no SSH tunnel and no registered redirect URI.
pub async fn login_with_pasted_code(
    app_key: &str,
    store: &TokenStore,
) -> Result<StoredCredentials> {
    let pkce = pkce::Pkce::generate();
    let url = oauth::authorize_url_for_paste(app_key, &pkce.challenge);

    println!("Open this URL to authorize dbsync:\n\n  {url}\n");
    println!("Approve the app, then paste the code Dropbox shows you.");
    print!("Code: ");
    io::stdout().flush().map_err(Error::from)?;

    let line = read_line().await?;
    let code = parse_pasted_code(&line)?;

    let client = OauthClient::new(app_key.to_string())?;
    let tokens = client.exchange_pasted_code(code, &pkce.verifier).await?;
    persist(tokens, store)
}

/// Read one line from stdin without blocking the async runtime.
async fn read_line() -> Result<String> {
    tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        io::stdin().read_line(&mut line).map(|_| line)
    })
    .await
    .map_err(|e| Error::Config(format!("could not read from stdin: {e}")))?
    .map_err(Error::from)
}

/// Clean up a pasted code and reject an empty one.
///
/// Terminals wrap and users paste generously, so surrounding whitespace is
/// expected; anything else is left alone rather than guessed at.
fn parse_pasted_code(line: &str) -> Result<&str> {
    let code = line.trim();
    if code.is_empty() {
        return Err(Error::Config("no code entered — aborting the login".into()));
    }
    Ok(code)
}

/// Turn a token response into stored credentials.
fn persist(tokens: oauth::TokenResponse, store: &TokenStore) -> Result<StoredCredentials> {
    let refresh_token = tokens
        .refresh_token
        .ok_or_else(|| Error::Config("Dropbox returned no refresh token".into()))?;
    let credentials = StoredCredentials {
        refresh_token,
        account_id: tokens.account_id,
    };
    store.save(&credentials)?;
    Ok(credentials)
}

/// A fresh, unguessable CSRF `state` value.
fn random_state() -> String {
    let mut bytes = [0u8; STATE_BYTES];
    getrandom::fill(&mut bytes).expect("OS CSPRNG unavailable");
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_values_are_unguessable_and_distinct() {
        let a = random_state();
        assert!(a.len() >= 22);
        assert_ne!(a, random_state());
    }

    #[test]
    fn a_pasted_code_is_trimmed_of_terminal_whitespace() {
        assert_eq!(parse_pasted_code("  abc123\n").unwrap(), "abc123");
    }

    /// Hitting enter at the prompt must fail loudly rather than send an empty
    /// code to Dropbox and report a confusing API error.
    #[test]
    fn an_empty_paste_is_rejected() {
        assert!(matches!(parse_pasted_code("  \n"), Err(Error::Config(_))));
    }

    #[test]
    fn state_values_are_url_safe() {
        assert!(
            random_state()
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }
}
