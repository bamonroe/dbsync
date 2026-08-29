//! Access-token supply: cache, expiry, and refresh-on-401.
//!
//! Every authenticated call goes through here rather than reading the token
//! store directly, so there is one place that knows when a token has gone stale
//! and one place that refreshes it.

use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use super::oauth::OauthClient;
use super::store::TokenStore;
use crate::error::Result;

/// Refresh this far ahead of the stated expiry, so a token cannot expire
/// mid-request just because the call took a moment to arrive.
const EXPIRY_SKEW: Duration = Duration::from_secs(300);

/// A cached access token and the moment it stops being usable.
struct CachedToken {
    value: String,
    usable_until: Instant,
}

impl CachedToken {
    fn is_usable(&self) -> bool {
        Instant::now() < self.usable_until
    }
}

/// Hands out access tokens, refreshing them as needed.
pub struct TokenProvider {
    oauth: OauthClient,
    store: TokenStore,
    cached: Mutex<Option<CachedToken>>,
}

impl TokenProvider {
    pub fn new(oauth: OauthClient, store: TokenStore) -> Self {
        Self {
            oauth,
            store,
            cached: Mutex::new(None),
        }
    }

    /// A usable access token, refreshed if the cached one has expired.
    ///
    /// Returns [`crate::Error::NotAuthenticated`] when no refresh token is
    /// stored, which is the signal to tell the user to run `dbsync auth login`.
    pub async fn access_token(&self) -> Result<String> {
        let mut cached = self.cached.lock().await;
        if let Some(token) = cached.as_ref()
            && token.is_usable()
        {
            return Ok(token.value.clone());
        }
        let fresh = self.fetch(&mut cached).await?;
        Ok(fresh)
    }

    /// Discard the cached token and fetch a new one.
    ///
    /// Call this after a 401: the token may have been revoked before its stated
    /// expiry, so the cache cannot be trusted.
    pub async fn force_refresh(&self) -> Result<String> {
        let mut cached = self.cached.lock().await;
        *cached = None;
        self.fetch(&mut cached).await
    }

    /// Refresh and populate the cache. Caller holds the lock.
    async fn fetch(&self, cached: &mut Option<CachedToken>) -> Result<String> {
        let credentials = self.store.load()?;
        let response = self.oauth.refresh(&credentials.refresh_token).await?;
        let usable_until = Instant::now() + expiry_window(response.expires_in);
        *cached = Some(CachedToken {
            value: response.access_token.clone(),
            usable_until,
        });
        Ok(response.access_token)
    }
}

/// How long a token whose stated lifetime is `expires_in` seconds may be used,
/// after subtracting the safety skew. Never negative.
fn expiry_window(expires_in: u64) -> Duration {
    Duration::from_secs(expires_in).saturating_sub(EXPIRY_SKEW)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A four-hour token is usable for four hours minus the skew.
    #[test]
    fn the_skew_is_subtracted_from_the_stated_lifetime() {
        assert_eq!(expiry_window(14400), Duration::from_secs(14400 - 300));
    }

    /// A token whose lifetime is shorter than the skew must be treated as
    /// already unusable rather than wrapping around to a huge duration.
    #[test]
    fn a_lifetime_shorter_than_the_skew_clamps_to_zero() {
        assert_eq!(expiry_window(60), Duration::ZERO);
        assert_eq!(expiry_window(0), Duration::ZERO);
    }

    #[test]
    fn a_token_past_its_window_is_not_usable() {
        let token = CachedToken {
            value: "at".into(),
            usable_until: Instant::now() - Duration::from_secs(1),
        };
        assert!(!token.is_usable());
    }

    #[test]
    fn a_token_inside_its_window_is_usable() {
        let token = CachedToken {
            value: "at".into(),
            usable_until: Instant::now() + Duration::from_secs(60),
        };
        assert!(token.is_usable());
    }
}
