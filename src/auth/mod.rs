//! OAuth2 PKCE login and token refresh.
//!
//! dbsync stores no app secret. It is a public OAuth client: the PKCE flow
//! (RFC 7636) proves possession of the verifier instead, so only the app key
//! ships in `config.toml`. The long-lived refresh token is kept outside the
//! repo with owner-only permissions; see [`store`].
//!
//! The login flow is:
//!
//! 1. Generate a PKCE verifier/challenge and a random `state`.
//! 2. Print the authorize URL for the user to open.
//! 3. Catch the redirect on loopback ([`loopback`]) and check `state` matches.
//! 4. Exchange the code plus the verifier for tokens ([`oauth`]).
//! 5. Persist the refresh token.
//!
//! Thereafter [`TokenProvider`] supplies access tokens and refreshes them.

mod loopback;
mod oauth;
mod pkce;
mod provider;
mod store;

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
    fn state_values_are_url_safe() {
        assert!(
            random_state()
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }
}
