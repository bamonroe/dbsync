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

`Reconciler::pull` is what a `Changed` signal turns into. It drains
`list_folder/continue` until `has_more` is false, applying each entry as it goes:

- **File** — downloaded, unless the state already holds that `rev` *and* the file is still on
  disk. The recorded entry is then re-derived from the file that landed, not from the
  metadata, so the local watcher does not read our own download as a local edit.
- **Folder** — created, since a change stream can deliver a file before its parent.
- **Tombstone** — the path is removed along with everything under it. Dropbox sends one
  tombstone for a deleted folder, not one per child.

Each entry is applied in **three phases**, and the split is structural rather than tidiness.
`decide` reads the state and works out what the entry calls for — skip, download, download
over preserved local bytes, mkdir, or delete. `fetch` performs it and reads no state at all.
`record` then writes the state to match. The exclusive borrow of the state is therefore held
either side of the download but never across it, which is the precondition for having more
than one download in flight: concurrent fetches cannot each hold `&mut` on one state.

Not every entry may overlap, though, so a page is first **partitioned into steps**
(`src/reconcile/schedule.rs`). File downloads are independent and go in a concurrent step;
tombstones and folder entries are serial. A tombstone is a **barrier** — everything in flight
drains before it is applied — because deleting a folder forgets and removes a whole subtree,
and a folder tombstone is the only notice its children are gone, so there is no per-child
event to order against. Entries for one path stay sequential within the concurrent step,
grouped by the same case-folding key the state uses, so two revisions of one file never race.
Folders are serial work but deliberately not barriers: a download creates its own parent
directories, so a file may safely precede its folder entry, and making every folder a barrier
would shred the parallelism of a first listing.

How much may then be in flight is decided in **bytes, not files** (`src/reconcile/budget.rs`).
A file count is the wrong unit — eight 1 KB files and eight 1 GB files are the same number and
nothing like the same load — and the size comes with the listing, so no estimating is needed.
Three limits interact: a byte budget (256 MB) bounds the total size in flight; a floor (16) is
admitted regardless of it, so one enormous file cannot serialise the small files queued behind
it; and a ceiling (48) caps the count however tiny the files are. Where the floor and the byte
budget conflict, the floor wins and the ceiling beats both, and a file larger than the whole
budget is charged the whole budget so it runs alone rather than never. The gate is written as a
pure counter with no network or disk dependency.

Measured against a live account on 2026-08-29, the defaults moved a first listing from **28
files/min** (sequential, 2.0 MiB/s over a 60s window of large files) to **304–461 files/min**
across two 60s windows of small files. The two windows are not the same workload, which is the
point of the byte budget: the sequential loop was bounded by one request at a time whatever the
size, so the gain shows up as file rate on small files and as link saturation on large ones.

The defaults themselves were then measured rather than guessed. On 2026-08-30, three cold syncs
of the same live account — each from an empty directory, sampled over the identical first 270
seconds so the file mix matches — gave:

| `max_concurrency` / `budget_bytes` | bytes @270s | files @270s | files/min |
|------------------------------------|-------------|-------------|-----------|
| 16 / 64 MiB (the old defaults)      | 484 MB      | 534         | 119       |
| **48 / 256 MiB (current)**          | **1.44 GB** | **2289**    | **509**   |
| 96 / 512 MiB                        | 1.16 GB     | 1420        | 316       |

Doubling past 48 was *slower on both axes*, and the daemon logged no rate limiting, retries or
warnings during that run — so the loss is contention among sockets rather than Dropbox pushing
back. Each row is a single run and the sync order is not byte-identical between them, so the
ratios carry real noise; what the table supports is the direction and the location of the knee,
not a precise multiplier. These are single-account figures on one link: the numbers justify the
shipped defaults, they do not generalise.

All three are **configurable** — `[download]` in `config.toml`, or the `DBSYNC_DOWNLOAD_*`
environment overrides (README documents what an operator would change and when). They are
tuning constants for a network-bound loop, and the right values depend on a link and an
account this repo cannot see. `Budget` still sanitises what it is handed, so a bad value can
never wedge the gate, but a zero budget or a ceiling below the floor is *rejected at config
load* instead: a startup error naming the key beats a pull that mysteriously admits one
download at a time. Whatever the values, parallelism stays inside the two invariants it may
not touch: the page barrier still gates the cursor advance (a page is applied in full before
its cursor is saved, so a crash re-delivers rather than skips), and tombstones still run on
the serial track as barriers.

#### One large file: ranged chunks

Parallelism *across* files does nothing for a single huge one. A 1 GB download is one HTTPS
stream at whatever one connection gives you, however large the byte budget is. So a file at or
above a threshold is split into fixed-size byte ranges and fetched concurrently into one
partial (`src/api/chunks.rs` plans the split, `src/api/download.rs` drives it). Below the
threshold nothing changes: the file arrives on one stream and is refetched whole if
interrupted, because splitting a small file four ways pays four round trips to save nothing.

Five decisions hold this together, and each exists to rule out a specific failure:

- **Every chunk addresses `rev:<rev>`, never the display path.** That is what makes concurrent
  chunks provably one revision. Against the display path, a remote edit landing mid-download
  would splice two revisions into one file — corruption that no later check could see, since
  the result is the right length and the wrong bytes.
- **Chunks are a fixed size, written at their true offsets** into a preallocated (sparse)
  file. Fixed size means chunk N's offset is implied by N rather than stored, so progress
  collapses to one bit per chunk; true offsets mean the bytes are right by construction
  whatever order they land in. Only the last chunk is short. The writes are *positional*
  (`write_all_at`), which is what lets every chunk share one open handle — no cursor exists
  for a concurrent chunk to move — and each is gathered into a 256 KiB block first, so a
  multi-megabyte chunk costs a few dozen disk dispatches rather than one per network frame.
  The single-stream path buffers the same way.
- **Progress is a sidecar bitmap, not a length** (`src/api/chunkmap.rs`). A length cannot
  express "chunks 0–3 and 5 landed, 4 did not". The sidecar's header carries the size, chunk
  size and count it was built for, and a mismatch discards it rather than reinterpreting bits
  that would point at the wrong bytes. A bit is fsynced before the chunk counts, so a crash
  may lose progress and refetch — never claim a chunk that is not on disk. The sidecar's name
  extends the partial's, so the startup sweep clears both together.
- **Completion is "every chunk present", never "length equals size"** (`src/api/partial.rs`).
  This is the sharpest edge in the design: a sparse file reaches its full length the moment the
  *last* chunk lands, so a length test would rename a file still full of holes into place. The
  sidecar is removed only after the rename succeeds, so a crash between the two leaves a
  complete partial the next attempt adopts rather than refetches.
- **The bytes are hashed against the revision's `content_hash` before the rename**
  (`src/api/download.rs`). Every check above proves a range *arrived*; none proves it is the
  right bytes, since a chunk written from a truncated or mis-served body still sets its bit.
  A mismatch abandons the partial — resuming into known-bad bytes would only reproduce the
  same hash — and fails the download with `Error::CorruptDownload`, so the path is retried
  from scratch rather than becoming the file. Metadata that carried no hash places the
  download unverified, since the alternative is refusing to sync it at all.

Chunk concurrency composes with the byte budget instead of multiplying with it. A file already
holds an admission for its own bytes, and it may only spend *that* reservation on its own
chunks — so the slots are `budgeted / chunk_size`, capped per file. A second independent pool
would multiply: sixteen files times eight chunks is a hundred and twenty-eight sockets from two
limits that each looked modest. A failing chunk is retried alone and the others keep their
bits; only a refused range invalidates the whole partial, because in that case the revision
will not serve what the plan assumed. The keys are in `[download]`; README documents which to
turn and when.

`src/reconcile/page.rs` is where the three come together. It walks the steps, and for each
concurrent step it decides every entry, awaits the fetches together under the budget, and then
records the outcomes **in listing order rather than completion order** — downloads finish out
of order by nature, and recording as they land would make the state file depend on the timing
of the network. Entries for one path are walked in rounds so a second revision always decides
against the state the first one recorded. The error policy is unchanged from the sequential
loop: one bad entry is logged and stepped over, and a later change re-delivers the path.

Two consequences worth knowing. The conflict check belongs to `decide`, not to the download,
because asking after the fetch would reopen the window that spurious conflicted copies came
through. And because `record` runs only once `fetch` succeeded, a failed removal leaves the
state describing what is still on disk — the alternative would leave those files untracked and
they would be uploaded straight back.

Three rules hold the whole thing together:

- **The cursor advances only after its page has been applied**, and is persisted immediately.
  A crash therefore re-delivers a page rather than skipping it, and re-applying a page is
  harmless because every operation is idempotent. Under concurrency this is no longer free —
  the page's slowest download gates the advance — but it stays the right trade, and a Dropbox
  cursor is opaque, so it cannot be positioned mid-page anyway. Entries *are* saved within a
  page; those interim saves only ever add files that really are on disk and never touch the
  cursor. The **interval between them scales with the tracked-file count** rather than being a
  flat hundred: the state file is rewritten whole, so a fixed interval makes a large pull
  quadratic in bytes written. Measured on a live account, a flat hundred at 8.5k tracked files
  already meant a 4 MB rewrite about once a second, pinning a core and starving the very
  download loop the checkpoint exists to protect; at 43k files it would have been ~19 MB a
  time. The interval is one entry per 64 tracked, clamped to [100, 1000] — the cap bounds what
  a crash can cost, and re-applying entries is idempotent and cheap. The whole-file rewrite
  itself has since gone, so the scaling interval is now belt-and-braces rather than the thing
  standing between a large pull and a stalled core — see the next invariant.
- **The state is saved incrementally, not rewritten.** `state.json` is a *snapshot* and
  `state.journal` beside it carries the changes made since (`src/state/journal.rs`).
  `SyncState` queues a record per mutation, `StateDb::save` appends the queued records, and
  `load` replays the journal on top of the snapshot. That makes a save cost what changed
  rather than what is stored — the difference between an O(entries) write per checkpoint and
  an O(changes) one. The snapshot is rewritten only when the journal passes `COMPACT_AFTER`
  (5,000 records) — a length the `Journal` keeps as a cached count, bumped on append and
  reset on clear, so deciding that on every save does not re-read the journal and put the
  O(n) straight back. The rename lands *before* the journal is cleared: a crash between the
  two replays records the snapshot already holds, which is harmless because replay is
  idempotent, whereas the other order would lose them. A torn final line — a crash
  mid-append — stops the replay rather than failing the load; the cost is the last few
  changes, which the next pull re-delivers.
- **A failed entry is recorded, not just logged.** One bad path must not stall the stream, so
  a failure is stepped over and the cursor still advances. That alone would make the file
  *silently absent*: the pull reports success, the log line scrolls away, and nothing
  re-delivers the path because the cursor has moved past its page — the file is missing and
  no one knows. So every failure is written into `state.json` alongside the entries
  (`src/state/failures.rs`), with its error, attempt count and first sighting, and the daemon
  warns with the count after each pull. `dbsync failures` lists them. Failures are classified
  **transient** or **permanent**, and the classification is deliberately biased: only a path
  the filesystem cannot represent (`ENAMETOOLONG`) is called permanent, and everything else is
  retried, because a wasted request is cheaper than a file that is never fetched again.
  `Reconciler::pull` re-attempts the transient ones through `files/get_metadata`, one path at
  a time, since the page they came from is long consumed and a cursor cannot be rewound. The
  candidate list is taken *before* the listing, so an entry that fails during this pull waits
  for the next one instead of being retried seconds after it failed; a path deleted remotely
  in the meantime is resolved rather than retried forever.
- **Both directions are recorded, and their retries never cross.** A failed *upload* is as
  silent as a failed download — the local edit exists only on this machine, and inotify will
  not fire a second time — so `Reconciler::push` records it too, and re-attempts the recorded
  uploads at the start of the next push. Each failure therefore carries a `direction`, and
  each retry pass filters on it: re-fetching a path whose upload failed would pull the remote
  copy over the local edit that never got sent, which is worse than the failure itself. The
  field defaults to `download` on load, so a state file written before it existed reads as
  what it could only have held.
- **The CLI asks the daemon; it does not write the state behind it.** `dbsync retry <path>`
  appends to a `retry-requests` file beside `state.json` (`src/state/requests.rs`) and the
  daemon takes and deletes it at the start of each pass. Only the CLI writes that file and
  only the daemon removes it, so there is nothing to race over — whereas editing `state.json`
  from a second process would simply be overwritten by the daemon's next whole-file save. A
  request is absorbed into the ordinary failure record, so it is retried under the same budget
  as everything else, and it revives a permanent entry deliberately.
- **A cursor reset is routine, not an error.** `pull` catches it, drops the cursor, and
  re-lists. The re-list *reconciles*: entries whose `rev` still matches are skipped, and
  anything in the state the listing does not mention was deleted remotely while we were
  offline, so it is removed locally too.
- **A name too long for the filesystem is shortened, not skipped.** Dropbox allows path
  components longer than Linux's 255-byte limit, so a legal remote name can have no legal
  local one — it used to fail with `ENAMETOOLONG` and the file was simply never there.
  `PathMapper::to_local` keeps a readable prefix, appends a fingerprint of the *whole*
  original name, and preserves the extension. The fingerprint is what makes it safe:
  truncation alone would map two names sharing a long prefix onto one file. Because the
  shortened name no longer says what the remote one was, the state carries an `aliases` index
  from local path to remote display path, and the upload direction consults it first —
  otherwise an edit would be uploaded as a second, wrongly-named remote file.
- **Only a folder's own entry says how it is capitalised.** Dropbox returns a `path_lower`
  and a `path_display`, but `path_display` is dependable only for its **last** component:
  ask for a deeply nested file and the folders above it can come back lowercased, while the
  same folder's own entry names it correctly. Creating a file's parents straight from its
  display path therefore froze the wrong case onto a case-sensitive filesystem — Dropbox
  never noticed, because to Dropbox the two names are one folder. `dircase::canonical`
  rebuilds every path through the folder casings recorded in `SyncState::folders` as their
  entries arrive; a listing hands out a folder before its contents, which is what makes that
  ordering work. A folder already on disk under the wrong name is *renamed* rather than
  duplicated (`apply::recase_existing`), except when both names exist — that is a collision
  between real files, not a rename to make silently.
- **The scratch name pays for its own suffix, not the real one.** A download writes to a
  `.<rev>.dbsync-partial` sibling, and the chunk map is named off *that*, so the scratch name
  is longer than the file's — a name sitting at exactly 255 bytes was legal at rest and still
  failed mid-download. `partial_path` shortens its own base against the reduced budget using
  the same fingerprinting helper (`paths::shorten_to`). Shortening `dest` instead would be
  the wrong trade: the rename lands at the end, so the real name only ever has to fit on its
  own, and shrinking it permanently to make room for a temporary suffix would mangle names
  that are perfectly legal.
- **Paths from Dropbox are untrusted.** `PathMapper` refuses any path that would escape the
  sync root rather than clamping it, and one unapplicable entry is logged and stepped over
  rather than stalling the stream behind it.

### Local-to-remote upload

`watcher::watch` subscribes to inotify recursively and emits a batch of paths once each has
been quiet for `watcher.debounce_ms`. Debouncing is not cosmetic: one editor save is a
create, several writes, a rename, and a chmod, and uploading each would race with itself.
Our own partial downloads and the state database are filtered out before anything is emitted.

A download streams to a `.dbsync-partial` sibling of its destination and is renamed into place
only once complete, so a torn file is never visible and the watcher never uploads one back.
The sibling placement is deliberate: `rename` is atomic only within a filesystem, so staging
somewhere central would silently become a copy and reopen that window. Partials outlive a
failed attempt on purpose, so the next one can resume (below), which means nothing deletes
them at the point of failure: `reconcile::sweep::partial_downloads` clears the strays at
startup — before the pull, since from then on an in-flight partial and an abandoned one look
identical.

An interrupted **chunked** download resumes rather than restarting: the partial and its chunk
map are kept, and the next attempt refetches only the ranges the map says are missing. Two
things make adopting an earlier attempt's bytes safe, and both are load-bearing:

- the partial's name carries the **revision** it was fetched from, so its bytes are never
  extended by a different revision's — a remote edit mid-download simply starts a new
  partial and leaves the old one for the sweep; and
- every request addresses that same immutable revision as `rev:<rev>`, not the display
  path, which would serve whatever is current.

A single-stream download does **not** resume: its progress is only a length, and a length
cannot be trusted after a crash — ext4 can leave the file at full size with an unsynced,
zero-filled tail, so appending past it would splice a hole into the middle of the finished
file. A file small enough for that path is cheap to refetch whole. A `416` means the revision
will not serve the ranges the plan assumed, so the partial describes a file that does not
exist: it is discarded and the download starts clean.

`Reconciler::push` then decides, per path, what actually happened:

- **Gone** — the remote copy is deleted, but only for a path the state was tracking. An
  unknown path that does not exist is nothing at all, not a deletion.
- **Metadata matches the state** — nothing happened; no upload, no hashing.
- **Metadata moved but the content hash did not** — a rewrite with identical bytes. The entry
  is re-stamped so the next check is cheap again, and nothing is uploaded.
- **Genuinely changed** — uploaded, single-shot below `SESSION_THRESHOLD` and as a chunked
  upload session above it.

Uploads are sent with `autorename: false` and, for a file we already know, `mode: update(rev)`
naming the revision we believe we are replacing. Dropbox therefore *refuses* a write that
would clobber a remote edit we have not seen yet, rather than silently winning.

### Conflicts

dbsync never resolves a conflict by picking a winner. When the two sides have diverged, the
local bytes are copied to `name (conflicted copy).ext` beside the original and the remote
version takes the original path. `src/reconcile/conflict.rs` owns the naming (numbered on
repeat, and a dotfile's leading dot is a name rather than an extension) and both entry points
into it:

- **Pushing** — Dropbox answers `update(rev)` with a 409 whose body says `conflict`, mapped to
  `Error::Conflict`. The copy is made and uploaded under its own name with `add`, and the
  original's state entry is re-stamped from disk. That re-stamp is what stops the watcher
  retrying the same losing write on every event.
- **Pulling** — before a download overwrites a path, `has_local_edit` asks whether the file on
  disk holds bytes we never sent: absent, metadata-matching, and hash-matching all say no; a
  file we never tracked says yes. If it does, the copy is made first.

The copy is a **copy, never a rename**. A rename would make the original path vanish, the
watcher would read that as a deletion, and dbsync would delete from Dropbox exactly the
version it was trying to protect.

### The daemon loop

`src/daemon` is where the concrete pieces meet; everything below it is written against traits,
so this is the only part that needs a real account.

Startup order is load-bearing. The daemon **pulls first**, before anything is watched: a first
run has no cursor, so that pull is the full listing that produces one, and a later run applies
whatever changed while it was down. Only then does it long-poll, on exactly that cursor, which
is what closes the window between the listing and the park. The local watcher starts last,
because the pull writes files and there is nothing to gain from feeding our own downloads into
the debouncer.

Both directions then run in **one** task. `Reconciler` owns the state database and is taken by
`&mut`, so a pull and a push cannot overlap — the select loop in `src/daemon/sync.rs` is what
serialises them, and that is what keeps a path from being written from both ends at once. Each
pull republishes its new cursor through the `CursorHandle`, or the long-poll loop would wake on
the change it has already been told about. A failed pull or push is logged, not fatal: the
cursor was never advanced past unapplied work, so the next notification retries it.

The select is `biased` on the shutdown future, so a constantly-changing directory cannot starve
the exit. `SIGINT` and `SIGTERM` mean the same thing — finish the operation in flight, drop the
event receiver, and abort the long-poll task, which may otherwise be parked for minutes.

Because both directions share that one task, **long blocking work never runs on a runtime
thread**. Three jobs are long enough to matter — content-hashing a file, saving the state (two
`fsync`s), and sweeping the synced tree — and each has an `async` twin that hands the work to
tokio's blocking pool through `src/blocking.rs`: `hash_file_off_thread`, `save_off_thread` and
`sweep::partial_downloads`. Left inline, a multi-gigabyte hash would stall every concurrent
download and the long-poll along with it. The state is *moved* into the blocking task and moved
back rather than cloned, since the caller holds it by `&mut` and a copy of a large account is
the cost being avoided.

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
| `src/blocking.rs` | Running blocking disk work off the runtime's worker threads | done |
| `src/notify/longpoll.rs` | The long-poll call: cursor in, outcome out | done |
| `src/notify/backoff.rs` | Capped exponential retry curve for failed polls | done |
| `src/notify/watch.rs` | The driving loop: holds the cursor, reconnects, emits `RemoteEvent` | done |
| `src/state/hash.rs` | Dropbox content hash (4 MiB SHA-256 tree) | done |
| `src/state/mod.rs` | Sync-state entry point; builds entries from local files | done |
| `src/state/entry.rs` | Per-file record and its change-detection predicates | done |
| `src/state/db.rs` | `SyncState` plus atomic load/save | done |
| `src/state/failures.rs` | Recording entries that could not be applied, and classifying them | done |
| `src/auth/pkce.rs` | RFC 7636 verifier/challenge generation | done |
| `src/auth/oauth.rs` | Authorize URL, code exchange, refresh | done |
| `src/auth/loopback.rs` | One-shot loopback listener for the redirect | done |
| `src/auth/store.rs` | Refresh token at rest, owner-only, atomic writes | done |
| `src/auth/provider.rs` | Access-token cache, expiry skew, refresh-on-401 | done |
| `src/api/client.rs` | Authenticated HTTP client, error mapping, refresh-on-401 | done |
| `src/api/metadata.rs` | The `.tag`-tagged file/folder/tombstone shapes | done |
| `src/api/get_metadata.rs` | Metadata for one path, which a retry needs | done |
| `src/api/list_folder.rs` | `list_folder` and `list_folder/continue` | done |
| `src/api/download.rs` | Streaming download with an atomic rename into place; picks whole-file or chunked | done |
| `src/api/range.rs` | The byte range a download asks for, and verifying the reply honoured it | done |
| `src/api/chunks.rs` | Planning one file's chunk layout, and its share of the byte budget | done |
| `src/api/chunkmap.rs` | The sidecar bitmap recording which chunks have landed | done |
| `src/api/partial.rs` | The partial file chunks are written into, and the completion gate | done |
| `src/api/upload.rs` | `files/upload`, chunked upload sessions, and delete | done |
| `src/reconcile/mod.rs` | `Reconciler`: both directions, owning the state between them | done |
| `src/reconcile/paths.rs` | Dropbox path ⇄ local path, with traversal refused | done |
| `src/reconcile/source.rs` | The `RemoteSource` trait the applier is written against | done |
| `src/reconcile/sink.rs` | The `RemoteSink` trait: upload and delete | done |
| `src/reconcile/local.rs` | Pushing one local path: upload, delete, or decide it is unchanged | done |
| `src/reconcile/apply.rs` | Applying one entry in three phases: decide, fetch, record | done |
| `src/reconcile/conflict.rs` | Conflicted-copy naming, and detecting an unsent local edit | done |
| `src/reconcile/budget.rs` | Byte-budget admission control for parallel downloads | done |
| `src/reconcile/schedule.rs` | Splitting a page into serial and concurrent steps | done |
| `src/reconcile/page.rs` | Applying one page, overlapping the downloads it safely can | done |
| `src/state/journal.rs` | The append-only journal of state changes, and folding it back into the snapshot | done |
| `src/state/requests.rs` | The retry-request queue the CLI writes and the daemon consumes | done |
| `src/reconcile/dircase.rs` | Rebuilding a remote path through the folder casings Dropbox only states on the folders' own entries | done |
| `src/reconcile/listing.rs` | Retrying wrappers around the two listing calls, so one dropped connection does not discard a full listing | done |
| `src/reconcile/retry.rs` | Choosing and resolving previously-failed entries for another attempt | done |
| `src/watcher/mod.rs` | inotify subscription, filtering, and batch emission | done |
| `src/watcher/debounce.rs` | Per-path quiet-period coalescing | done |
| `src/daemon/mod.rs` | Building the components from config and the startup order | done |
| `src/daemon/sync.rs` | The one loop that serialises pulls and pushes | done |
| `src/daemon/shutdown.rs` | SIGINT/SIGTERM as a single shutdown future | done |

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

There is a second way in, `auth login --paste-code`, which omits `redirect_uri` from the
authorize request entirely. Dropbox then renders the code in the browser instead of
redirecting, and the user pastes it at a prompt. This is the flow for a headless host: the
approving browser is on a different machine, so a loopback listener here is unreachable no
matter what port it binds. The exchange must then *also* omit `redirect_uri` — OAuth2 requires
the two requests to agree, and sending one only at exchange time is a mismatch.

That flow drops the `state` parameter, which is correct rather than a shortcut: `state` binds
a redirect to the flow that started it, and there is no redirect. The PKCE verifier still
binds the exchange, and the code travels through the user rather than over the network.

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
- **A pull that fails part-way is resumed, not restarted.** The cursor is persisted page by
  page, so the daemon logs a failed startup pull and carries on into the watch loop rather
  than exiting; the next notification resumes from the saved cursor. Two cases stay fatal:
  no cursor at all (nothing to long-poll on) and a rejected credential.
- **A listing call is retried, not surrendered.** A page of downloads that fails costs a
  page; a `list_folder`/`continue` that fails unwinds the whole pull, and on a deliberately
  cleared cursor a full-account listing is hours of work. `src/reconcile/listing.rs` retries
  transient failures (dropped connections, 5xx, rate limits) with exponential backoff and
  passes `CursorReset` straight through, because that one is an instruction to re-list.
- **Content identity uses Dropbox's content hash** (the 4 MiB block SHA-256 tree), not mtime,
  so an echo of our own upload is recognised and not re-applied.
- **A chunked download completes on every chunk being present**, never on the file's length.
  Chunks are written at their true offsets, so the partial is full length as soon as the last
  one lands; renaming on length would publish a file with holes in the middle.
- **Conflicts never destroy data.** Divergent edits produce a `filename (conflicted copy).ext`
  alongside the original, matching native-client behaviour.
