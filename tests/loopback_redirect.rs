//! End-to-end check of the OAuth loopback listener over a real socket.
//!
//! The unit tests cover query parsing; this covers the part they cannot — that
//! the listener binds, survives a browser's stray requests, and answers the
//! redirect with a real HTTP response.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Port used by these tests. Not the production port, so a real `auth login`
/// running on the same machine cannot collide with the test.
const TEST_PORT: u16 = 53699;

async fn get(port: u16, target: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream
        .write_all(format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

/// A browser typically fetches /favicon.ico before or alongside the redirect.
/// The listener must shrug that off and keep waiting rather than giving up.
#[tokio::test]
async fn captures_the_redirect_after_ignoring_an_unrelated_request() {
    let server = tokio::spawn(dbsync::auth::wait_for_redirect_on(TEST_PORT));

    // Give the listener a moment to bind before connecting.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let noise = get(TEST_PORT, "/favicon.ico").await;
    assert!(noise.starts_with("HTTP/1.1 404"), "got: {noise}");

    let answer = get(TEST_PORT, "/?code=the-code&state=the-state").await;
    assert!(answer.starts_with("HTTP/1.1 200"), "got: {answer}");
    assert!(answer.contains("close this tab"));

    let redirect = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("listener did not return")
        .expect("listener panicked")
        .expect("listener errored");

    assert_eq!(redirect.code, "the-code");
    assert_eq!(redirect.state, "the-state");
}
