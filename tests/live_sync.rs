//! End-to-end sync against a real Dropbox test account.
//!
//! Everything below the daemon wiring is already covered against the in-memory
//! [`FakeRemote`]; what no unit test can prove is that the real service agrees
//! with our reading of it. So this suite talks to an actual account and pins
//! the three claims the project makes:
//!
//! 1. a local write reaches Dropbox,
//! 2. a remote write reaches the local directory, and the long-poll endpoint —
//!    not a timer — is what announces it, and
//! 3. edits on both sides of the same file leave a conflicted copy rather than
//!    a silent loss.
//!
//! **These tests are skipped unless credentials are in the environment**, so a
//! plain `cargo test` on a laptop with no account still passes. See the "Live
//! sync tests" section of `README.md` for the variables and how to get them.
//!
//! Each test works inside its own uniquely-named folder under the configured
//! remote root and deletes it afterwards, so concurrent runs and a shared test
//! account do not collide.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dbsync::api::{ApiClient, RemoteEntry, WriteMode};
use dbsync::auth::{OauthClient, StoredCredentials, TokenProvider, TokenStore};
use dbsync::notify::{LongpollClient, LongpollOutcome};
use dbsync::reconcile::{PathMapper, Reconciler};
use dbsync::state::StateDb;

/// How long a remote change may take to surface locally before we call it a
/// failure. Generous: this is a network test, and the assertion is "seconds,
/// not minutes", not a latency benchmark.
const SETTLE: Duration = Duration::from_secs(60);

/// Long-poll timeout for the notification test. The endpoint's minimum is 30s;
/// we want the *change* to end the call, not the timeout, so keep it short
/// enough that a hung test fails inside `SETTLE`.
const LONGPOLL_TIMEOUT_SECS: u64 = 30;

/// A live account plus a scratch folder inside it, torn down on drop.
struct Live {
    /// The reconciler under test, rooted at `local`.
    reconciler: Reconciler<ApiClient>,
    /// A second client, used to play the part of the *other* machine.
    other: ApiClient,
    /// The scratch local directory. Held so the temp dir outlives the test.
    _tmp: tempfile::TempDir,
    local: PathBuf,
    /// The scratch remote folder, e.g. `/dbsync-test/run-a1b2c3`.
    remote_root: String,
}

impl Live {
    /// Build a harness, or return `None` when the environment has no account.
    ///
    /// Reading credentials rather than running the OAuth flow is deliberate:
    /// the flow needs a browser, and CI has none.
    fn from_env(label: &str) -> Option<Self> {
        let app_key = std::env::var("DBSYNC_TEST_APP_KEY").ok()?;
        let refresh_token = std::env::var("DBSYNC_TEST_REFRESH_TOKEN").ok()?;
        let base =
            std::env::var("DBSYNC_TEST_REMOTE_ROOT").unwrap_or_else(|_| "/dbsync-test".to_string());

        let tmp = tempfile::tempdir().expect("temp dir");
        let local = tmp.path().join("root");
        std::fs::create_dir_all(&local).expect("local root");

        // The provider reads its refresh token from a store on disk, so hand it
        // one inside the temp dir — never the operator's real credentials file.
        let store = TokenStore::at(tmp.path().join("credentials.json"));
        store
            .save(&StoredCredentials {
                refresh_token,
                account_id: None,
            })
            .expect("stage credentials");

        let oauth = OauthClient::new(app_key).expect("oauth client");
        let tokens = Arc::new(TokenProvider::new(oauth, store));
        let api = ApiClient::new(Arc::clone(&tokens)).expect("api client");
        let other = ApiClient::new(tokens).expect("second api client");

        // Unique per test *and* per run, so a shared account tolerates both a
        // parallel `cargo test` and a previous run that died before cleanup.
        let remote_root = format!("{}/{}-{}", base.trim_end_matches('/'), label, run_id());

        let paths = PathMapper::new(&local, &remote_root);
        let db = StateDb::at(tmp.path().join("state.json"));
        let state = db.load().expect("empty state");

        Some(Self {
            reconciler: Reconciler::new(api, paths, db, state),
            other,
            _tmp: tmp,
            local,
            remote_root,
        })
    }

    /// The local path for a name inside the scratch root.
    fn local_path(&self, name: &str) -> PathBuf {
        self.local.join(name)
    }

    /// The remote path for a name inside the scratch folder.
    fn remote_path(&self, name: &str) -> String {
        format!("{}/{}", self.remote_root, name)
    }

    /// Write `content` locally and push it, as the watcher would.
    async fn write_and_push(&mut self, name: &str, content: &str) {
        let local = self.local_path(name);
        std::fs::write(&local, content).expect("write local file");
        let push = self.reconciler.push(&[local]).await.expect("push");
        assert_eq!(push.uploaded, 1, "expected {name} to upload");
    }

    /// Upload `content` straight to Dropbox, bypassing the reconciler — this is
    /// the stand-in for an edit made on another machine.
    async fn remote_write(&self, name: &str, content: &str, mode: WriteMode) {
        let staged = self._tmp.path().join("outbound");
        std::fs::write(&staged, content).expect("stage remote content");
        self.other
            .upload(&self.remote_path(name), &staged, &mode)
            .await
            .expect("upload as the other machine");
    }

    /// Every file Dropbox currently holds under the scratch folder.
    async fn remote_names(&self) -> Vec<String> {
        let page = self
            .other
            .list_folder(&self.remote_root)
            .await
            .expect("list scratch folder");
        page.entries
            .iter()
            .filter_map(|entry| match entry {
                RemoteEntry::File(file) => Some(file.path_display.clone()),
                _ => None,
            })
            .map(|path| path.rsplit('/').next().unwrap_or_default().to_string())
            .collect()
    }

    /// Delete the scratch folder. Best effort: a failure here would mask the
    /// test's own result, so it is reported and swallowed.
    async fn cleanup(&self) {
        if let Err(error) = self.other.delete(&self.remote_root).await {
            eprintln!(
                "live test: could not clean up {}: {error}",
                self.remote_root
            );
        }
    }
}

/// A short random-ish suffix. Derived from the clock rather than a RNG crate;
/// uniqueness across concurrent runs on one machine is all that is needed.
fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos();
    format!("{:x}", nanos as u64)
}

/// Skip the test body when there are no credentials, saying so out loud.
///
/// A silently-passing skipped test is worse than no test; the printed line is
/// what tells an operator these three never actually ran.
macro_rules! live {
    ($label:expr) => {
        match Live::from_env($label) {
            Some(live) => live,
            None => {
                eprintln!(
                    "skipping live sync test `{}`: set DBSYNC_TEST_APP_KEY and \
                     DBSYNC_TEST_REFRESH_TOKEN to run it (see README.md)",
                    $label
                );
                return;
            }
        }
    };
}

/// Poll `check` until it holds, or fail after [`SETTLE`].
async fn eventually<F>(what: &str, mut check: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out after {SETTLE:?} waiting for {what}");
}

/// Does `path` hold exactly these bytes?
fn holds(path: &Path, content: &str) -> bool {
    std::fs::read_to_string(path)
        .map(|got| got == content)
        .unwrap_or(false)
}

/// The upload direction: a file written locally exists in the real account.
#[tokio::test]
async fn a_local_write_reaches_dropbox() {
    let mut live = live!("upload");

    live.write_and_push("hello.txt", "written locally").await;

    let names = live.remote_names().await;
    assert!(
        names.iter().any(|name| name == "hello.txt"),
        "hello.txt missing from the account; saw {names:?}"
    );

    live.cleanup().await;
}

/// The download direction, and the claim the project is named for: a change
/// made elsewhere ends a parked long-poll, and the follow-up pull writes it to
/// disk.
#[tokio::test]
async fn a_remote_write_arrives_by_long_poll() {
    let mut live = live!("longpoll");

    // Establish the folder and a cursor to park on. The pull is what produces
    // the cursor — there is nothing to long-poll before it.
    live.write_and_push("seed.txt", "seed").await;
    live.reconciler.pull().await.expect("initial pull");
    let cursor = live
        .reconciler
        .cursor()
        .expect("a pull always leaves a cursor")
        .to_string();

    let poller = LongpollClient::new(LONGPOLL_TIMEOUT_SECS).expect("longpoll client");
    let parked = tokio::spawn(async move { poller.wait(&cursor).await });

    // Give the park a moment to actually be in flight, so the change we make
    // next is one the endpoint has to notice rather than one it already saw.
    tokio::time::sleep(Duration::from_secs(2)).await;
    live.remote_write("from-elsewhere.txt", "written remotely", WriteMode::Add)
        .await;

    let outcome = tokio::time::timeout(SETTLE, parked)
        .await
        .expect("long-poll did not return in time")
        .expect("long-poll task panicked")
        .expect("long-poll call failed");
    assert_eq!(
        outcome,
        LongpollOutcome::Changed,
        "the endpoint should report a change, not time out"
    );

    live.reconciler
        .pull()
        .await
        .expect("pull after notification");
    let arrived = live.local_path("from-elsewhere.txt");
    eventually("the remote file to land locally", || {
        holds(&arrived, "written remotely")
    })
    .await;

    live.cleanup().await;
}

/// Divergence: the same file edited on both sides keeps both versions.
///
/// The local edit is deliberately *not* pushed before the pull — that is the
/// window a real daemon has, between an inotify event and its upload, and it is
/// the only way both sides can legitimately hold different bytes.
#[tokio::test]
async fn diverging_edits_leave_a_conflicted_copy() {
    let mut live = live!("conflict");

    live.write_and_push("shared.txt", "original").await;
    live.reconciler.pull().await.expect("initial pull");

    // The other machine's edit. `Add` would be refused now that the path
    // exists, so overwrite it the way a second client would.
    live.remote_write(
        "shared.txt",
        "edited remotely",
        WriteMode::Update(current_rev(&live, "shared.txt").await),
    )
    .await;

    // Our edit, still unsent.
    let local = live.local_path("shared.txt");
    std::fs::write(&local, "edited locally").expect("local edit");

    live.reconciler
        .pull()
        .await
        .expect("pull over a local edit");

    // The remote version wins the real path; ours is preserved beside it.
    eventually("the remote version to land", || {
        holds(&local, "edited remotely")
    })
    .await;
    let preserved = conflicted_copies(&live.local);
    assert_eq!(
        preserved.len(),
        1,
        "expected exactly one conflicted copy in {:?}, found {preserved:?}",
        live.local
    );
    assert!(
        holds(&preserved[0], "edited locally"),
        "the conflicted copy should hold the unsent local bytes"
    );

    live.cleanup().await;
}

/// Dropbox's current revision for a file in the scratch folder.
async fn current_rev(live: &Live, name: &str) -> String {
    let page = live
        .other
        .list_folder(&live.remote_root)
        .await
        .expect("list scratch folder");
    page.entries
        .iter()
        .find_map(|entry| match entry {
            RemoteEntry::File(file) if file.path_display.ends_with(name) => Some(file.rev.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{name} is not in the account"))
}

/// Every `… (conflicted copy)…` file directly under `root`.
fn conflicted_copies(root: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("read local root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().contains("conflicted copy"))
                .unwrap_or(false)
        })
        .collect();
    found.sort();
    found
}
