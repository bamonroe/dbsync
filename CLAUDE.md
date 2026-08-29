# CLAUDE.md

Guidance for Claude Code instances working in this repository.

> **This is a template.** Replace the bracketed placeholders (`[…]`) with the real
> project's specifics, delete the sections that don't apply, and keep the working
> practices below — they encode the owner's standing preferences and apply to every
> project regardless of stack.

## Start here: the documentation map (hub-and-spoke)

This repo keeps documentation **de-duplicated** — every fact has exactly **one**
authoritative home. When you need to know or change something, go to its owner below;
don't restate a fact in a second file (link to the owner instead). This table is the
**hub**: read it first, then follow the spoke you need. The point of hub-and-spoke is that
the detail lives out in the spoke docs, so this file (and the context window) stays small —
you pull in a spoke only when the task touches it, instead of reading everything every turn.

| You want to know / change…                       | Authoritative home        |
|--------------------------------------------------|---------------------------|
| **What to do next** (active tasks only)          | `TODO.toml` (via the `todo` skill) |
| **What's already done** (completed-work archive) | `FINISHED.toml` (via the `todo` skill) |
| **How to work here** (conventions, decisions)    | `CLAUDE.md` (this file)   |
| **How the system works internally** (data flow, layout) | `docs/architecture.md` |
| **How a user runs/uses it** (setup, build & run) | `README.md`               |
| **External tools & build pipelines** (Android, deploy, device harnesses) | `docs/tools.md` |
| **[Any other spoke — protocol, commands, API…]** | `docs/[name].md`          |

**Two classes of fact:**

1. **Code-derived facts** (env vars, wire messages, error codes, generated lists) are
   owned by the code. Where practical, make the docs a *mirror* and add a **drift test that
   fails the build** when they fall out of sync, so a stale doc is caught mechanically
   rather than by discipline. Regenerate generated docs from their source; never
   hand-maintain a second copy a test doesn't check.
2. **Narrative facts** (status, "verified live", roadmap history) can't be tested, so they
   live in **one** place only and the update rules below keep that single copy current.

## What this project is

**dbsync** is a Dropbox sync client for Linux: a long-running daemon that keeps a local
directory and a Dropbox account in sync, behaving like the native desktop client. Its
defining choice is that remote changes arrive by **push, not polling** — the daemon parks on
Dropbox's long-poll notification endpoint and reacts within seconds of a remote change,
while a local filesystem watcher (inotify) drives the upload direction.

It is built in **Rust** (`tokio` async runtime), and runs as a single binary — there is no
server half and no public HTTP endpoint (see the webhooks note in `docs/architecture.md`).
Rust is the house default for long-lived network services here; see the technology defaults
below.

## Architecture — see `docs/architecture.md`

The internals live in **`docs/architecture.md`**: [data-flow, key components, the repository
layout, and the load-bearing design decisions]. Read it when you're changing the data path
or the core components. When you make a design decision, **record it in the owning doc** so
it isn't re-litigated later.

## Build, run & repository layout — see `docs/architecture.md` and `README.md`

The **repository layout** (every package/module and what it does) lives in
`docs/architecture.md`; **how to build and run** it lives in `README.md`. Don't restate
either here.

```sh
# The canonical build + check loop for this project:
cargo build                              # build
cargo clippy --all-targets -- -D warnings && cargo fmt --check   # static checks
cargo test                               # tests
```

## External tools & build pipelines — see `docs/tools.md`

Tooling that isn't part of this repo's own build — external build pipelines, deploy scripts,
device/emulator harnesses, and the like — is documented in **`docs/tools.md`**. That file
indexes each tool and, when the detail is large, links out to its own doc. **Keep
tool-specific setup out of this file:** add a pointer here and put the specifics in the tools
doc, so the main instructions stay small and each pipeline has one home.

For example, a project that ships an **Android app** records its whole build/test pipeline —
the containerized builder, the emulator harness, device install, store publish — in
`docs/tools.md` (or a dedicated `docs/android.md` it links), not inline here.

---

## Development style guide — technology defaults (standing preferences)

These are the owner's standing defaults for *how* things get built. They apply to every
project regardless of domain; deviate only when a project has a concrete, written reason to,
and record that reason in this file.

### Develop inside containers — Docker first, then Podman

**Do development, builds, and runs inside containers, not against the host toolchain.** A
fresh checkout should build and run through a container so the environment is reproducible
and the host stays clean.

- **Prefer Docker.** It is the default container runtime for this project — write the
  `Dockerfile`/`compose.yaml` for Docker and assume `docker`/`docker compose` in scripts and
  docs.
- **Fall back to Podman** when Docker isn't available or appropriate. Keep the setup
  Podman-compatible (rootless-friendly, no Docker-only Compose features) so `podman` /
  `podman compose` is a drop-in; note any command differences where they matter.
- Don't rely on host-installed language toolchains or services — pin them in the container so
  the build is the same everywhere.

### Web servers are written in Rust

**If this project needs a web server, write it in Rust.** Rust is the default for any HTTP /
web-server component — pick a mature async stack (e.g. `axum`/`tokio` or `actix-web`) and keep
the server code idiomatic. Use another language for the server only with a written reason
recorded here.

### Keep the code split up — no mono-files, no giant functions

**Break the project into many small pieces.** This is a hard preference: it exists to keep
the code navigable, documentable, and unit-testable.

- **No single mono-file.** Don't pour the whole project into one giant `main.rs` (or one
  huge module in any language). Split it along real boundaries into separate modules/files,
  each with a clear, single responsibility.
- **No mono-module.** Within a file, don't stack everything into one blob — break each module
  into distinct types/classes and functions with focused jobs.
- **Keep functions small — roughly 100 lines max.** If a function grows past ~100 lines,
  extract helpers. Smaller units are easier to name, document, and cover with unit tests.
- The point is navigation, documentation, and testing: a reader should find the right file by
  its name, and each unit should be small enough to test in isolation.

---

## Working practices (standing preferences — keep these in every project)

### Git: commit atomically, at will and frequently — and push freely

This repo is the safety net. **Commit atomically, at will, and frequently.** You have
standing authorization to commit your own work **without asking first** — don't wait to be
told, and never let work pile up uncommitted.

- **Atomic commits**: one logical change per commit. A bug fix, a feature, a doc update,
  and a refactor are separate commits — don't bundle unrelated changes. Commit the smallest
  coherent unit that builds/tests clean.
- Make the change → build/vet/test it → **commit**. Prefer many small commits over one large
  one; it keeps history bisectable and easy to revert (`git revert <sha>`).
- Write a concise **imperative** subject that says *why*, not just *what*
  (`fix: input bar behind nav bar`, `feat: read-last command`).
- **Commit before risky or large changes**, so there's always a clean point to return to.
  It's fine to commit work-in-progress on a branch; prefer a branch for anything speculative
  so the main branch stays runnable.
- **Push freely and liberally.** You have standing authorization to `git push` without
  asking first — don't let local commits sit unpushed. Push after committing (or after a
  short run of related commits); keeping the remote current is part of "done."
- Never commit secrets, databases, or build artifacts — `.gitignore` covers the routine ones.

### A feature isn't done until it's documented

**Documentation is part of the feature, not a follow-up.** Write it *during* the feature
work, or immediately after — never defer it to "later," and never ship code without it.

- Every new feature gets full user-facing documentation in `README.md` as part of the same
  work.
- Keep the single-source-of-truth spoke docs in sync in the same pass (a new command, wire
  message, config var, etc. goes in its owning doc per the map above).
- Docs land in the **same commit** as the feature (or an immediately-following commit) — a
  feature commit with no accompanying documentation is incomplete.
- Any script or command needed to build, deploy, run, back up, or operate this project
  **must be written down** — put reusable steps in a checked-in script, and reference every
  such script from both `README.md` (how an operator runs it) and this file (how it fits).
  Nothing load-bearing should live only in someone's shell history.

### `TODO.toml` (active) + `FINISHED.toml` (archive) — keep them current

`TODO.toml` (repo root) is the single source of truth for **active** work and the **journal
for what to build next**; `FINISHED.toml` is the archive of **completed** work. Splitting the
two keeps `TODO.toml` a short list of what's actually in flight instead of an ever-growing
pile of done items. Status lives in these two files only — not here and not in `README.md`
(all link to them).

Both are **TOML**, not Markdown, and each task carries structured metadata (`id`, `status`,
`level`, `category`, `urgency`, `order`, `created`/`completed`, `tags`). **Drive them through the
`todo` skill** (`scripts/todo.sh <command>`, documented in `.claude/skills/todo/SKILL.md`)
rather than hand-editing, so ids, ordering, and metadata stay consistent and the files stay
diff-friendly. The skill also answers the "how many tasks / bugs / features are left" kind
of question (`scripts/todo.sh stats`).

- **Update both in the same commit that changes the work they describe:** add proposed
  features/tests with `scripts/todo.sh add …`; drop dropped items with
  `scripts/todo.sh remove <id> --reason "…"`.
- **When a task is fully finished** (built, tested, documented), run
  `scripts/todo.sh done <id>` to move it out of `TODO.toml` and into `FINISHED.toml`,
  newest-first and dated. Don't leave completed items sitting in `TODO.toml`, and don't
  delete the history; `FINISHED.toml` only grows.
- Use `TODO.toml` to journal next steps: when you finish something and notice the next
  feature, `add` it rather than losing it. A future session reads `TODO.toml` first to know
  where to pick up, and `FINISHED.toml` to see what already shipped.
- A stale `TODO.toml`/`FINISHED.toml` means the change isn't done — same rule as the docs.
  Run `scripts/todo.sh validate` if you ever hand-edit them.
- **Project-specific per-task flags:** if this project wants boolean hints on each task (e.g.
  "needs a rebuild", "needs a staging deploy"), add them to `FLAGS` in
  `.claude/skills/todo/scripts/store.py` and document **what each one means for whoever works
  the task** here, so they're load-bearing rather than decorative. `[List them here, or say
  the project uses none.]`

### Token discipline — keep the context small

- **Read in slices, not whole files.** Use `grep`/`glob` to find the target, then `Read`
  with `offset`/`limit` around it. Only read a whole file when you genuinely need all of it.
  Never re-read a file you just edited — the edit already confirmed the new state.
- **Delegate broad searches to `Explore` subagents.** Any "where/how is X done" sweep across
  many files goes to a subagent that reads in its own context and returns just the
  conclusion, so the file dumps never land in this conversation.
- **Don't restate; link.** Point at the owning doc (per the map above) instead of pasting its
  content into a reply or a new file. This is the whole reason for hub-and-spoke.
- **Prefer targeted output.** Pipe long command output through `head`/`tail`/`grep`; don't
  cat whole logs or list huge trees.
