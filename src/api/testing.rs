//! Test-only helpers for the API layer.

/// A `reqwest::Response` built from parts, with no network behind it.
///
/// `reqwest` has no public constructor, so the only way to hand a response to
/// the parsing and verification code under test is to convert one from `http`.
/// That conversion is fiddly enough to be worth having in exactly one place.
pub fn fake_response(
    status: u16,
    headers: &[(&str, &str)],
    body: impl Into<Vec<u8>>,
) -> reqwest::Response {
    let mut builder = http::Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    reqwest::Response::from(builder.body(body.into()).unwrap())
}
