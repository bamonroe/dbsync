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

Both directions funnel through a single **reconciler** so that a change is never applied in
both directions at once; the reconciler owns the local↔remote state database and is the only
component allowed to write it.

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
| `src/state/hash.rs` | Dropbox content hash (4 MiB SHA-256 tree) | done |
| `src/state/mod.rs` | Local sync database; the only writer of sync state | stub |
| `src/auth.rs` | OAuth2 PKCE flow, refresh-token storage, refresh on 401 | stub |
| `src/api.rs` | Typed wrappers over the authenticated endpoints | stub |
| `src/watcher.rs` | inotify subscription plus debounce/coalescing | stub |
| `src/reconcile.rs` | Change-application engine: conflicts, ordering, retries | stub |
| `src/daemon.rs` | Process lifecycle, signals, wiring the above together | stub |

The `notify` crate (inotify bindings) is renamed to `notify_fs` in `Cargo.toml`, because the
name collides with our own `crate::notify` module.

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
