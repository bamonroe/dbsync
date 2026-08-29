//! OAuth2 PKCE login and token refresh.
//!
//! dbsync stores no app secret: the PKCE flow means only the app key ships in
//! `config.toml`, and the long-lived refresh token is kept outside the repo.
//!
//! Not yet implemented — see the
//! `implement-oauth2-pkce-login-and-token-refresh` task in `TODO.toml`.
