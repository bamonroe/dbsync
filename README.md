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
- A Dropbox app created at <https://www.dropbox.com/developers/apps>, with:
  - the `files.metadata.read`, `files.content.read` and `files.content.write` scopes —
    `files.metadata.read` is required for the folder listing and the long-poll endpoint the
    daemon is built around, so it will not start without it;
  - **Allow public clients (Implicit Grant & PKCE)** enabled — dbsync is a PKCE-only public
    client and Dropbox rejects the authorize request outright without it;
  - `http://localhost:53682` added as a redirect URI — needed only for the browser-redirect
    login below, not for `--paste-code`.

  dbsync has no built-in app key to borrow. Other clients ship one shared across all their
  users, but those apps are registered as confidential clients with a secret, so their keys
  cannot be reused by a PKCE client like this one.

## Build

```sh
docker compose build
```

## Configure

Copy the example config and fill in your app key:

```sh
cp config.example.toml config.toml
```

`local_root` is the directory being synced, `/data/storage/dbsync` by default. In a
container that is the path *inside* the container; set `DBSYNC_LOCAL_ROOT` to point the
bind mount at a different host directory (it defaults to the same path on the host, so
the two match out of the box):

```sh
DBSYNC_LOCAL_ROOT=/srv/dropbox docker compose up -d dbsync
```

### Download concurrency

Remote changes are fetched in parallel, and how much may be in flight is decided in
**bytes, not files** — a hundred tiny files overlap freely while one huge file runs alone.
The `[download]` section tunes that:

| Key               | Default | What it does                                                        |
|-------------------|---------|---------------------------------------------------------------------|
| `budget_bytes`    | 256 MiB | Total size of downloads allowed in flight at once.                  |
| `min_concurrency` | 16      | Downloads admitted regardless of the budget, so one enormous file cannot serialise the small files behind it. |
| `max_concurrency` | 48      | Hard cap on downloads in flight, however small the files.           |
| `chunk_min_size`  | 8 MiB   | Files at least this large are fetched as several byte ranges at once, not one stream. |
| `chunk_size`      | 8 MiB   | The nominal size of each range.                                     |
| `max_chunks`      | 64      | The most ranges one file is ever split into; past this the ranges grow instead. |
| `chunk_concurrency` | 8     | The most ranges of a *single* file in flight at once.               |

When to change them:

- **A slow or metered link** — lower `budget_bytes`. The budget is what bounds how much of a
  large file's traffic is in flight, so it is the knob that keeps a thin pipe responsive.
- **Many small files on a fast link** — raise `max_concurrency`. Small files never exhaust the
  byte budget, so the count cap is what limits them. The default of 48 is a measured optimum on
  one real account; raising it to 96 made a cold sync *slower*, with no rate limiting in the
  logs, so treat it as a knob to measure rather than to turn up. See `docs/architecture.md`.
- **Downloads are stalling behind one huge file** — raise `min_concurrency`.
- **One large file is slow on a fast link** — that is what the chunk keys are for. A single
  stream runs at one connection's speed however large the byte budget, so a big file is split
  into `chunk_size` ranges fetched `chunk_concurrency` at a time. The per-file limit does not
  multiply with `max_concurrency`: a file may only spend the bytes its own admission reserved,
  so `chunk_concurrency` is a ceiling on that share rather than a second pool of sockets.
- **Lots of medium files being split pointlessly** — raise `chunk_min_size`. Splitting only
  pays once a file is large enough that the extra round trips are lost in the transfer.

`budget_bytes` of 0, `min_concurrency` of 0, or a `max_concurrency` below `min_concurrency`
are refused at startup with an error, rather than leaving the daemon unable to admit anything.
The chunk keys are checked the same way: `chunk_size`, `max_chunks` and `chunk_concurrency`
must each be at least 1, and `chunk_min_size` at least `chunk_size` — a threshold below one
chunk would split files that cannot usefully be split.

Each key also has a `DBSYNC_`-prefixed environment override, which wins over the file — handy
for tuning a running container without editing its config:

```sh
DBSYNC_DOWNLOAD_BUDGET_BYTES=16777216 \
DBSYNC_DOWNLOAD_MAX_CONCURRENCY=32 \
  docker compose up -d dbsync
```

`config.example.toml` lists every option the daemon accepts — a drift test
(`tests/config_drift.rs`) fails the build if it ever gains or loses a key
relative to the `Config` struct, so the example is always the complete reference.

Check that the daemon reads it as you expect:

```sh
docker compose run --rm dbsync check
```

## Link your Dropbox account

Two ways in, depending on whether a browser can reach this machine.

Scopes are fixed at the moment the token is granted, so if you change an app's permissions
later you must re-run `auth login` — an existing refresh token keeps the old, narrower set
and the daemon fails with a `missing scope` API error.

### Headless / over SSH — paste the code

```sh
docker compose run --rm -it dbsync auth login --paste-code
```

This prints a URL. Open it in a browser **anywhere** — your laptop, your phone — approve the
app, and Dropbox shows you an authorization code. Paste it at the `Code:` prompt and dbsync
exchanges it for a refresh token.

Nothing listens on a port, so there is no SSH tunnel to set up and no redirect URI to
register. `-it` is required: the prompt reads from stdin.

### Local machine — catch the redirect

```sh
docker compose run --rm -p 53682:53682 dbsync auth login
```

Open the printed URL and the browser is redirected back to `http://localhost:53682`, where
dbsync catches the code. This needs `http://localhost:53682` registered as a redirect URI on
the app, and the `-p` flag — without it the redirect cannot reach the container.

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

### Files that failed to sync

A transfer can fail without the sync failing — a dropped connection, a Dropbox 5xx, or a
filename longer than Linux allows. When that happens the entry is **recorded**, not just
logged, because a failed transfer is otherwise invisible: the pull or push reports success
and the file is simply not where it should be. Both directions are recorded: a download that
never landed leaves the file missing from disk, and an upload that never went leaves your
local edit existing only on this machine.

```sh
dbsync failures                # everything currently out of sync
dbsync failures --permanent    # only the ones that need you to act
dbsync failures --retryable    # only the ones a retry may still fix
```

Each line shows the direction (`download` or `upload`), the path, the last error, and how
many times it has been attempted. The record
lives in `state.json`, so it survives a restart, and the daemon logs a warning naming the
count after each pull rather than finishing quietly.

Failures are split into two kinds:

- **Retryable** — a network error or a server-side fault. Every pull re-attempts the failed
  downloads and every push re-attempts the failed uploads, so a transient problem heals itself
  without you doing anything. The two never cross: re-fetching a path whose upload failed
  would pull the remote copy over the local edit that never got sent. An entry that failed
  during *this* pass waits for the next one rather than being hammered immediately. A path deleted from Dropbox in the meantime is
  dropped from the list instead of being retried forever.
- **Permanent** — currently only a path the local filesystem cannot represent
  (`File name too long`). Retrying cannot help, so these are never re-attempted and are listed
  first. Renaming the file in Dropbox is the fix; it then syncs normally on the next pull.

A climbing `attempts` count on a retryable entry is the signal that something is wrong beyond
bad luck.

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
