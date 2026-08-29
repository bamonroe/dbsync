# dbsync

A realtime Dropbox sync daemon for Linux. It keeps a local folder and a Dropbox account in
sync and behaves like the native desktop client — remote changes arrive by **push** (Dropbox's
long-poll notification endpoint) rather than by periodic polling, so edits made elsewhere show
up locally within seconds.

> **Status:** working. Both sync directions are wired into the daemon, so `dbsync run`
> pulls remote changes and uploads local ones. Conflict handling is still missing — a file
> edited on both sides at once is not yet split into a conflicted copy. See `TODO.toml` for
> what's active and `FINISHED.toml` for what has shipped.

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
