//! PKCE verifier and challenge generation (RFC 7636).
//!
//! PKCE is what lets dbsync ship without an app secret: the client proves it
//! is the same party that started the flow by presenting the verifier whose
//! SHA-256 it committed to up front.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

/// Bytes of entropy behind the verifier. 32 bytes base64url-encodes to 43
/// characters, the minimum RFC 7636 permits.
const VERIFIER_BYTES: usize = 32;

/// A PKCE verifier and the challenge derived from it.
#[derive(Debug, Clone)]
pub struct Pkce {
    /// The secret. Sent only on the token exchange, never in the browser URL.
    pub verifier: String,
    /// The public commitment. Sent in the authorize URL.
    pub challenge: String,
}

impl Pkce {
    /// Generate a fresh verifier/challenge pair from the OS CSPRNG.
    pub fn generate() -> Self {
        let mut bytes = [0u8; VERIFIER_BYTES];
        getrandom::fill(&mut bytes).expect("OS CSPRNG unavailable");
        Self::from_verifier(URL_SAFE_NO_PAD.encode(bytes))
    }

    /// Derive the challenge for a given verifier (`S256` method).
    pub fn from_verifier(verifier: String) -> Self {
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        Self {
            verifier,
            challenge,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example from RFC 7636 appendix B.
    #[test]
    fn matches_the_rfc_7636_test_vector() {
        let pkce = Pkce::from_verifier("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk".to_string());
        assert_eq!(
            pkce.challenge,
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn generated_verifiers_meet_the_rfc_length_floor() {
        let pkce = Pkce::generate();
        assert!((43..=128).contains(&pkce.verifier.len()));
    }

    #[test]
    fn generated_verifiers_are_url_safe() {
        let pkce = Pkce::generate();
        assert!(
            pkce.verifier
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }

    #[test]
    fn two_generated_pairs_differ() {
        assert_ne!(Pkce::generate().verifier, Pkce::generate().verifier);
    }

    #[test]
    fn the_challenge_is_derived_from_the_verifier() {
        let pkce = Pkce::generate();
        assert_eq!(
            pkce.challenge,
            Pkce::from_verifier(pkce.verifier.clone()).challenge
        );
    }
}
