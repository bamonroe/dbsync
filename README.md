# dbsync

A realtime Dropbox sync daemon for Linux. It keeps a local folder and a Dropbox account in
sync and behaves like the native desktop client — remote changes arrive by **push** (Dropbox's
long-poll notification endpoint) rather than by periodic polling, so edits made elsewhere show
up locally within seconds.

> **Status:** early. The crate, container build, config loading, content hashing, and the
> long-poll client are in place; **syncing is not implemented yet** — `dbsync run` will tell
> you so and exit. See `TODO.toml` for what's active and `FINISHED.toml` for what has shipped.

## How it works

The daemon parks on Dropbox's `/2/files/list_folder/longpoll` endpoint, which blocks until
something changes, then fetches the change list and applies it locally. In the other
direction an inotify watcher picks up local edits and uploads them. Details and the reasoning
behind that choice are in [`docs/architecture.md`](docs/architecture.md).

## Requirements

- Docker (or Podman) — the build and run both happen in a container; no host Rust toolchain
  is needed.
- A Dropbox app created at <https://www.dropbox.com/developers/apps> with the
  `files.content.read` and `files.content.write` scopes.

## Build

```sh
docker compose build
```

## Configure

Copy the example config and fill in your app key:

```sh
cp config.example.toml config.toml
```

Check that the daemon reads it as you expect:

```sh
docker compose run --rm dbsync check
```

Then link the account — this runs the OAuth flow and stores a refresh token:

```sh
docker compose run --rm dbsync auth login     # not implemented yet
```

## Run

```sh
docker compose up dbsync          # foreground
docker compose up -d dbsync       # detached
```

The daemon syncs the directory named in `config.toml`. Logs go to stdout; raise verbosity
with `RUST_LOG=debug`.

## Development

```sh
cargo build                                                       # build
cargo clippy --all-targets -- -D warnings && cargo fmt --check     # static checks
cargo test                                                        # tests
```

Conventions, standing preferences, and the documentation map live in
[`CLAUDE.md`](CLAUDE.md).
