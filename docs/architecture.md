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

## Components (planned module layout)

Per the "keep the code split up" rule in `CLAUDE.md`, each of these is its own module, not a
mono-file:

- `auth` — OAuth2 PKCE flow, refresh-token storage, token refresh on 401.
- `api` — typed wrappers over the Dropbox HTTP endpoints (metadata, upload, download,
  upload sessions for large files).
- `notify` — the long-poll loop: cursor in, change signal out; owns backoff and reconnect.
- `watcher` — inotify subscription plus debounce/coalescing of local events.
- `state` — the local sync database: path → (rev, content hash, local mtime, size).
- `reconcile` — the change-application engine: conflict detection, ordering, retries.
- `daemon` — process lifecycle, config, signal handling, wiring the above together.

## Load-bearing invariants

- **The cursor is the source of truth for remote position** and is persisted with the state
  database. Losing it means a full re-list, not data loss.
- **Cursors expire.** A `409 reset` from `continue` is expected, not exceptional: drop the
  cursor, re-run `/files/list_folder`, and reconcile against local state.
- **Content identity uses Dropbox's content hash** (the 4 MiB block SHA-256 tree), not mtime,
  so an echo of our own upload is recognised and not re-applied.
- **Conflicts never destroy data.** Divergent edits produce a `filename (conflicted copy).ext`
  alongside the original, matching native-client behaviour.
