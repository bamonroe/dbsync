# dbsync

A realtime Dropbox sync daemon for Linux. It keeps a local folder and a Dropbox account in
sync and behaves like the native desktop client — remote changes arrive by **push** (Dropbox's
long-poll notification endpoint) rather than by periodic polling, so edits made elsewhere show
up locally within seconds.

> **Status:** working. Both sync directions are wired into the daemon, so `dbsync run`
> pulls remote changes, uploads local ones, and keeps conflicted copies when the two
> diverge. See `TODO.toml` for what's active and `FINISHED.toml` for what has shipped.

## How it works

The daemon parks on Dropbox's `/2/files/list_folder/longpoll` endpoint, which blocks until
something changes, then fetches the change list and applies it locally. In the other
direction an inotify watcher picks up local edits and uploads them. Details and the reasoning
behind that choice are in [`docs/architecture.md`](docs/architecture.md).

## Requirements

- Docker (or Podman) — the build and run both happen in a container; no host Rust toolchain
  is needed.
- A Dropbox app created at <https://www.dropbox.com/developers/apps> with the
  `files.content.read` and `files.content.write` scopes, and
  `http://localhost:53682` added as a redirect URI.

## Build

```sh
docker compose build
```

## Configure

Copy the example config and fill in your app key:

```sh
cp config.example.toml config.toml
```

`config.example.toml` lists every option the daemon accepts — a drift test
(`tests/config_drift.rs`) fails the build if it ever gains or loses a key
relative to the `Config` struct, so the example is always the complete reference.

Check that the daemon reads it as you expect:

```sh
docker compose run --rm dbsync check
```

## Link your Dropbox account

```sh
docker compose run --rm -p 53682:53682 dbsync auth login
```

This prints a URL. Open it, approve the app, and the browser is redirected back to
`http://localhost:53682`, where dbsync catches the authorization code and exchanges it for a
refresh token. The `-p` flag is required: without it the redirect cannot reach the container.

dbsync uses OAuth2 **PKCE**, so no app secret is ever stored — only your app key, in
`config.toml`. The refresh token is written to `~/.local/share/dbsync/credentials.json` with
owner-only permissions (in the container, the `dbsync-auth` volume), never into the repo or
the image.

Check the link at any time:

```sh
docker compose run --rm dbsync auth status
```

If you revoke the app from the Dropbox account page, `auth login` again to re-link.

## Run

```sh
docker compose up dbsync          # foreground
docker compose up -d dbsync       # detached
```

Or, without a container, `cargo run -- run --config config.toml`.

On startup the daemon applies everything the remote changed while it was down, then parks on
the long-poll endpoint and watches the local directory. Logs go to stdout; raise verbosity
with `RUST_LOG=debug`. `SIGINT` (Ctrl-C) or `SIGTERM` stops it after the operation in flight
finishes, so the state file is never left describing a half-applied change.

### Conflicts

If a file is edited locally and on Dropbox at the same time, dbsync keeps both. Your local
version is copied to `name (conflicted copy).ext` next to the original — numbered
`(conflicted copy 2)` and so on if it happens again — and the original path gets the version
from Dropbox. Nothing is overwritten and nothing is discarded; you resolve it by hand, and the
conflicted copy syncs up like any other file.

The first run has no cursor and lists the whole folder, which can take a while on a large
account; later runs resume from the saved cursor in `state.json`.

## Development

```sh
cargo build                                                       # build
cargo clippy --all-targets -- -D warnings && cargo fmt --check     # static checks
cargo test                                                        # tests
```

Conventions, standing preferences, and the documentation map live in
[`CLAUDE.md`](CLAUDE.md).

### Live sync tests

`tests/live_sync.rs` proves the round trip against a **real Dropbox account**: a
local write reaches Dropbox, a remote write comes back down and it is the
long-poll endpoint that announces it, and edits on both sides leave a conflicted
copy. Everything else is covered against an in-memory fake, so these three are
the only tests that need the network.

They **skip themselves** when the environment has no account — `cargo test` on a
fresh checkout passes without one, printing a `skipping live sync test …` line
per test so you can tell they did not run.

To run them, use a **throwaway Dropbox account** (the tests create and delete
folders in it) and export:

| Variable                    | Meaning                                                  |
|-----------------------------|----------------------------------------------------------|
| `DBSYNC_TEST_APP_KEY`       | App key of a Dropbox app, as in [Configure](#configure). |
| `DBSYNC_TEST_REFRESH_TOKEN` | A refresh token for the test account (required).         |
| `DBSYNC_TEST_REMOTE_ROOT`   | Folder to work under. Optional; defaults to `/dbsync-test`. |

Get the refresh token by linking the test account once with `dbsync auth login`
and reading it out of the credentials file that step writes (its path is printed
by `dbsync auth status`). Treat it as a password — it does not expire.

```sh
export DBSYNC_TEST_APP_KEY=… DBSYNC_TEST_REFRESH_TOKEN=…
cargo test --test live_sync -- --nocapture
```

Each test works inside its own uniquely-named folder and deletes it afterwards,
so a shared account tolerates concurrent runs; a run killed mid-test can leave a
stray `…-<hex>` folder behind to sweep by hand.
