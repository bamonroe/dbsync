//! The OAuth2 token endpoint: authorization-code exchange and refresh.

use serde::Deserialize;

use crate::error::{Error, Result};

/// Where the user is sent to approve the app.
const AUTHORIZE_URL: &str = "https://www.dropbox.com/oauth2/authorize";
/// Where codes and refresh tokens are exchanged for access tokens.
const TOKEN_URL: &str = "https://api.dropboxapi.com/oauth2/token";

/// Fixed loopback port, so the redirect URI is a constant that can be
/// registered once in the Dropbox app console.
pub const REDIRECT_PORT: u16 = 53682;

/// The redirect URI to register for this app.
pub fn redirect_uri() -> String {
    format!("http://localhost:{REDIRECT_PORT}")
}

/// Build the URL the user opens to approve the app.
///
/// `token_access_type=offline` is what makes Dropbox return a refresh token;
/// without it the daemon would need re-approval every few hours.
pub fn authorize_url(app_key: &str, challenge: &str, state: &str) -> String {
    let encode = urlencoding::encode;
    format!(
        "{}&redirect_uri={}&state={}",
        base_authorize_url(app_key, challenge),
        encode(&redirect_uri()),
        encode(state),
    )
}

/// Build the authorize URL for the paste-the-code flow.
///
/// Omitting `redirect_uri` is what makes Dropbox display the authorization
/// code on screen instead of redirecting a browser, which is the only workable
/// shape on a headless box: the approving browser is on another machine, so
/// nothing can reach a loopback listener here. No `state` either — it guards
/// against a forged redirect, and there is no redirect to forge.
pub fn authorize_url_for_paste(app_key: &str, challenge: &str) -> String {
    base_authorize_url(app_key, challenge)
}

/// The parameters both flows share.
fn base_authorize_url(app_key: &str, challenge: &str) -> String {
    let encode = urlencoding::encode;
    format!(
        "{AUTHORIZE_URL}?client_id={}&response_type=code&token_access_type=offline\
         &code_challenge={}&code_challenge_method=S256",
        encode(app_key),
        encode(challenge),
    )
}

/// A successful response from the token endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    /// Present on the initial code exchange; absent when refreshing.
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Lifetime of the access token, in seconds.
    pub expires_in: u64,
    #[serde(default)]
    pub account_id: Option<String>,
}

/// Talks to the OAuth2 token endpoint. Holds no credentials of its own.
pub struct OauthClient {
    http: reqwest::Client,
    app_key: String,
}

impl OauthClient {
    pub fn new(app_key: String) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
            app_key,
        })
    }

    /// Exchange an authorization code plus its PKCE verifier for tokens.
    pub async fn exchange_code(&self, code: &str, verifier: &str) -> Result<TokenResponse> {
        let redirect = redirect_uri();
        self.post(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", &self.app_key),
            ("code_verifier", verifier),
            ("redirect_uri", &redirect),
        ])
        .await
    }

    /// Exchange a code the user pasted in by hand.
    ///
    /// `redirect_uri` is omitted deliberately: OAuth2 requires the exchange to
    /// echo whatever the authorize request sent, and the paste flow sent none.
    /// Including one here would be rejected as a mismatch.
    pub async fn exchange_pasted_code(&self, code: &str, verifier: &str) -> Result<TokenResponse> {
        self.post(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", &self.app_key),
            ("code_verifier", verifier),
        ])
        .await
    }

    /// Trade a refresh token for a fresh access token.
    ///
    /// No client secret is sent: PKCE public clients authenticate with the
    /// client id alone.
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenResponse> {
        self.post(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", &self.app_key),
        ])
        .await
    }

    async fn post(&self, form: &[(&str, &str)]) -> Result<TokenResponse> {
        let response = self.http.post(TOKEN_URL).form(form).send().await?;
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            // A rejected refresh token means the user revoked access or the
            // grant expired: re-running `auth login` is the only way back.
            if status == reqwest::StatusCode::BAD_REQUEST
                || status == reqwest::StatusCode::UNAUTHORIZED
            {
                return Err(Error::NotAuthenticated);
            }
            return Err(Error::Api {
                status: status.as_u16(),
                message,
            });
        }
        response.json().await.map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_authorize_url_carries_the_pkce_challenge() {
        let url = authorize_url("appkey", "chal-123", "st");
        assert!(url.starts_with(AUTHORIZE_URL));
        assert!(url.contains("code_challenge=chal-123"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("client_id=appkey"));
        assert!(url.contains("response_type=code"));
    }

    /// Without `token_access_type=offline` Dropbox returns no refresh token,
    /// and a daemon that must survive restarts would be unusable.
    #[test]
    fn the_authorize_url_requests_offline_access() {
        assert!(authorize_url("k", "c", "s").contains("token_access_type=offline"));
    }

    #[test]
    fn the_authorize_url_percent_encodes_the_redirect() {
        let url = authorize_url("k", "c", "s");
        assert!(url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A53682"));
    }

    /// The absent `redirect_uri` is the whole point: it is what makes Dropbox
    /// display the code instead of redirecting.
    #[test]
    fn the_paste_authorize_url_sends_no_redirect_uri() {
        let url = authorize_url_for_paste("appkey", "chal-123");
        assert!(!url.contains("redirect_uri"));
        assert!(!url.contains("state="));
    }

    #[test]
    fn the_paste_authorize_url_still_carries_pkce_and_offline_access() {
        let url = authorize_url_for_paste("appkey", "chal-123");
        assert!(url.starts_with(AUTHORIZE_URL));
        assert!(url.contains("client_id=appkey"));
        assert!(url.contains("code_challenge=chal-123"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("token_access_type=offline"));
        assert!(url.contains("response_type=code"));
    }

    #[test]
    fn special_characters_in_the_challenge_are_encoded() {
        assert!(authorize_url("a b", "c+d", "s").contains("code_challenge=c%2Bd"));
    }

    #[test]
    fn the_redirect_uri_uses_the_fixed_loopback_port() {
        assert_eq!(redirect_uri(), "http://localhost:53682");
    }

    /// The refresh response omits `refresh_token`; the stored one stays valid.
    #[test]
    fn a_refresh_response_parses_without_a_refresh_token() {
        let parsed: TokenResponse =
            serde_json::from_str(r#"{"access_token":"at","expires_in":14400}"#).unwrap();
        assert_eq!(parsed.access_token, "at");
        assert!(parsed.refresh_token.is_none());
    }

    #[test]
    fn an_initial_exchange_response_parses_with_all_fields() {
        let parsed: TokenResponse = serde_json::from_str(
            r#"{"access_token":"at","refresh_token":"rt","expires_in":14400,"account_id":"dbid:x"}"#,
        )
        .unwrap();
        assert_eq!(parsed.refresh_token.as_deref(), Some("rt"));
        assert_eq!(parsed.account_id.as_deref(), Some("dbid:x"));
    }
}
