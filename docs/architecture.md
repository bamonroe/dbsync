# Architecture

How **dbsync** works internally. For *what it is*, see `CLAUDE.md`; for *how to run it*, see
`README.md`.

## The core decision: long-poll, not webhooks, not polling

Dropbox offers three ways to learn that files changed. Only one of them suits a daemon
running on a user's own machine:

| Mechanism | Endpoint | Needs a public server? | Verdict |
|---|---|---|---|
| Poll | `/2/files/list_folder/continue` | No | Too slow / wasteful as the primary path |
| **Long-poll** | `/2/files/list_folder/longpoll` | **No** | **What we use** |
| Webhooks | app-registered HTTPS callback | **Yes** | Rejected — a desktop daemon has no public URL |

**Decision:** remote change detection is built on `/2/files/list_folder/longpoll`. The call
blocks until a change is detected under the cursor's folder or until its timeout elapses,
so the daemon sits idle at near-zero cost and wakes within seconds of a real change. This is
the "push" behaviour the project is named for.

Two properties of the endpoint shape the design:

- It lives on a **separate host**, `notify.dropboxapi.com`, and takes **no `Authorization`
  header** — the cursor is the capability. It therefore needs its own HTTP client, distinct
  from the authenticated `api.dropboxapi.com` / `content.dropboxapi.com` clients.
- It is a *notification*, not a payload. A `changes: true` response carries no file data; the
  daemon must follow up with `/2/files/list_folder/continue` to learn what actually changed.
- Responses may include a `backoff` value, which must be honoured before re-polling.

## Data flow

```
                  ┌──────────────── remote direction ────────────────┐
  notify.dropboxapi.com/longpoll(cursor)  ──blocks──▶  changes: true
                  │                                          │
                  │                                          ▼
                  │                        /files/list_folder/continue(cursor)
                  │                                          │
                  └──── new cursor ◀── reconciler ◀── change entries
                                            │
                                            ▼
                                     local filesystem
                                            ▲
                                            │
  inotify watcher ──▶ debounce ──▶ reconciler ──▶ /files/upload[_session]
                  └──────────────── local direction ─────────────────┘
```

### The notify loop's contract

`notify::channel(poller, cursor)` returns three things: the loop itself, a `CursorHandle`,
and a `RemoteEvent` receiver. The split keeps the loop ignorant of file data — it only ever
nudges.

- The loop emits `RemoteEvent::Changed` when Dropbox reports changes. The reconciler answers
  by calling `/files/list_folder/continue` and publishing the resulting cursor back through
  the `CursorHandle`; every poll re-reads the handle, so the next call uses it.
- A `backoff` in a successful response is slept verbatim. A *failed* poll retries on a
  capped exponential curve (1s doubling to 60s) that resets on the next success, so a
  network outage never kills the daemon and never escalates past a one-minute retry.
- `Error::CursorReset` becomes `RemoteEvent::CursorReset`, after which the loop **idles**
  rather than re-polling a cursor Dropbox has already rejected. It resumes the moment the
  reconciler publishes a fresh one (see the cursor-reset rule below).
- Dropping the event receiver is the shutdown signal: the loop returns at its next
  iteration.

Both directions funnel through a single **reconciler** so that a change is never applied in
both directions at once; the reconciler owns the local↔remote state database and is the only
component allowed to write it.

### Remote-to-local application

`RemoteApplier::pull` is what a `Changed` signal turns into. It drains
`list_folder/continue` until `has_more` is false, applying each entry as it goes:

- **File** — downloaded, unless the state already holds that `rev` *and* the file is still on
  disk. The recorded entry is then re-derived from the file that landed, not from the
  metadata, so the local watcher does not read our own download as a local edit.
- **Folder** — created, since a change stream can deliver a file before its parent.
- **Tombstone** — the path is removed along with everything under it. Dropbox sends one
  tombstone for a deleted folder, not one per child.

Three rules hold the whole thing together:

- **The cursor advances only after its page has been applied**, and is persisted immediately.
  A crash therefore re-delivers a page rather than skipping it, and re-applying a page is
  harmless because every operation is idempotent.
- **A cursor reset is routine, not an error.** `pull` catches it, drops the cursor, and
  re-lists. The re-list *reconciles*: entries whose `rev` still matches are skipped, and
  anything in the state the listing does not mention was deleted remotely while we were
  offline, so it is removed locally too.
- **Paths from Dropbox are untrusted.** `PathMapper` refuses any path that would escape the
  sync root rather than clamping it, and one unapplicable entry is logged and stepped over
  rather than stalling the stream behind it.

## Repository layout

Per the "keep the code split up" rule in `CLAUDE.md`, each of these is its own module, not a
mono-file. "stub" means the file exists with its doc comment and its `TODO.toml` task, but no
implementation yet.

| Path | Responsibility | State |
|---|---|---|
| `src/main.rs` | CLI parsing and dispatch only (`run`, `auth`, `check`) | done |
| `src/lib.rs` | Library root; re-exports `Config`, `Error`, `Result` | done |
| `src/config.rs` | Load and validate `config.toml` | done |
| `src/error.rs` | Crate-wide `Error`/`Result` | done |
| `src/notify/longpoll.rs` | The long-poll call: cursor in, outcome out | done |
| `src/notify/backoff.rs` | Capped exponential retry curve for failed polls | done |
| `src/notify/watch.rs` | The driving loop: holds the cursor, reconnects, emits `RemoteEvent` | done |
| `src/state/hash.rs` | Dropbox content hash (4 MiB SHA-256 tree) | done |
| `src/state/mod.rs` | Sync-state entry point; builds entries from local files | done |
| `src/state/entry.rs` | Per-file record and its change-detection predicates | done |
| `src/state/db.rs` | `SyncState` plus atomic load/save | done |
| `src/auth/pkce.rs` | RFC 7636 verifier/challenge generation | done |
| `src/auth/oauth.rs` | Authorize URL, code exchange, refresh | done |
| `src/auth/loopback.rs` | One-shot loopback listener for the redirect | done |
| `src/auth/store.rs` | Refresh token at rest, owner-only, atomic writes | done |
| `src/auth/provider.rs` | Access-token cache, expiry skew, refresh-on-401 | done |
| `src/api/client.rs` | Authenticated HTTP client, error mapping, refresh-on-401 | done |
| `src/api/metadata.rs` | The `.tag`-tagged file/folder/tombstone shapes | done |
| `src/api/list_folder.rs` | `list_folder` and `list_folder/continue` | done |
| `src/api/download.rs` | Streaming download with an atomic rename into place | done |
| `src/reconcile/mod.rs` | `RemoteApplier`: drains the change stream, advances the cursor | remote direction done |
| `src/reconcile/paths.rs` | Dropbox path ⇄ local path, with traversal refused | done |
| `src/reconcile/source.rs` | The `RemoteSource` trait the applier is written against | done |
| `src/reconcile/apply.rs` | Applying one entry: download, mkdir, delete subtree | done |
| `src/watcher.rs` | inotify subscription plus debounce/coalescing | stub |
| `src/daemon.rs` | Process lifecycle, signals, wiring the above together | stub |

The `notify` crate (inotify bindings) is renamed to `notify_fs` in `Cargo.toml`, because the
name collides with our own `crate::notify` module.

## Authentication

dbsync is a **public OAuth client**: it ships no app secret, so PKCE (RFC 7636) is what proves
the token exchange comes from the same party that began the flow. Only the app key lives in
`config.toml`.

`token_access_type=offline` on the authorize URL is load-bearing — without it Dropbox returns
no refresh token, and a daemon that must survive restarts would need re-approval every few
hours.

The redirect is caught by a **one-shot loopback listener** on the fixed port **53682**, which
is why that port is a constant rather than an ephemeral one: the redirect URI has to be
registered in the Dropbox app console ahead of time. The listener binds `127.0.0.1` only and
exits as soon as it has a code. This is not a contradiction of the no-public-server rule
above — nothing is ever reachable off the loopback interface.

Two safety properties worth keeping:

- The `state` parameter is random per login and checked on return, so a third party cannot
  steer the listener into exchanging a code it did not request.
- Access tokens are refreshed **five minutes before** their stated expiry, so a token cannot
  expire between the check and the request that uses it. `TokenProvider::force_refresh` exists
  for the 401 case, where a token was revoked ahead of schedule and the cache is untrustworthy.

## Sync state

The state is a single JSON document at `$XDG_DATA_HOME/dbsync/state.json`, holding the folder
cursor plus one entry per file: `rev`, content hash, local mtime, size, and the display path.
Rewriting it whole is the right trade at this size — one atomic replace is far easier to
reason about than incremental updates that could half-apply.

**Atomic save** means: write a temporary file, `fsync` it, `rename` over the target, then
`fsync` the parent directory. The first sync makes the bytes durable; the last makes the
rename itself durable. A crash therefore leaves either the complete old state or the complete
new one, and a leftover `.tmp` is ignored on load.

**Entries are keyed by lowercased path.** Dropbox treats paths case-insensitively, so
`/Photos/Cat.jpg` and `/photos/cat.jpg` are one file; keying on the lowercase form stops a
case change from being read as a second file. The original casing is preserved separately for
display.

**Change detection is two-tier.** `metadata_matches` compares size and mtime, which is cheap
and lets a scan skip unchanged files without reading them. It is a pre-filter, not proof — an
editor can rewrite a file to the same size within the same timestamp granularity — so when it
reports a difference the reconciler hashes to find out what actually happened.

A state file whose `version` is newer than this build understands is refused rather than
misread.

## Container build

`Dockerfile` is a two-stage build: a pinned `rust:1.90-slim-bookworm` builder and a
`debian:bookworm-slim` runtime carrying only the binary and a CA bundle. Dependencies are
compiled in their own layer ahead of `COPY src`, so a source-only change does not rebuild the
dependency tree. The runtime runs as uid 1000 rather than root; `compose.yaml` matches that
uid so synced files keep their ownership, and keeps credentials in a named volume rather than
in the image.

## Load-bearing invariants

- **The cursor is the source of truth for remote position** and is persisted with the state
  database. Losing it means a full re-list, not data loss.
- **Cursors expire.** A `409 reset` from `continue` is expected, not exceptional: drop the
  cursor, re-run `/files/list_folder`, and reconcile against local state.
- **Content identity uses Dropbox's content hash** (the 4 MiB block SHA-256 tree), not mtime,
  so an echo of our own upload is recognised and not re-applied.
- **Conflicts never destroy data.** Divergent edits produce a `filename (conflicted copy).ext`
  alongside the original, matching native-client behaviour.
