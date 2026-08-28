---
name: todo
description: >-
  Read and edit this project's task list. Use whenever the user says "add this
  to the todo", "what's next / what's on the todo", "mark X done", "edit the
  todo", "how many tasks / how many bugs are left", or otherwise wants to query
  or change TODO.toml / FINISHED.toml. The task list is TOML, not Markdown —
  drive it through this skill's CLI so ids, ordering, and metadata stay
  consistent.
---

# Todo skill

The active task list lives in **`TODO.toml`** and the completed-work archive in
**`FINISHED.toml`** at the repo root. They are the single source of truth for
what's in flight and what has shipped (see `CLAUDE.md` → documentation map).
Both are plain TOML, edited through a small dependency-free Python CLI so every
write stays diff-friendly and every task carries full metadata.

## How to run it

```sh
scripts/todo.sh <command> [options]          # operator wrapper (from repo root)
# or directly:
python3 .claude/skills/todo/scripts/todo.py <command> [options]
```

Run `scripts/todo.sh <command> --help` for a command's options.

## The schema

Each file is a document: a `[meta]` header (title, purpose, rules) then an
array of `[[task]]` tables. A task carries **more** metadata rather than less:

| Field         | Where            | Meaning                                             |
|---------------|------------------|-----------------------------------------------------|
| `id`          | both             | stable kebab-case slug (the handle for every command) |
| `title`       | both             | short one-line summary                              |
| `description` | both             | the detail                                          |
| `status`      | both             | active · in-progress · blocked (TODO) / finished (archive) |
| `level`       | both             | task · scope · epic — how big it is, and how to work it |
| `category`    | both             | feature · bug · docs · refactor · test · chore     |
| `urgency`     | TODO             | low · normal · high · critical                     |
| `order`       | TODO             | manual sort key (10, 20, 30…); lower = sooner       |
| `created`     | both             | date the task was added (`YYYY-MM-DD`)             |
| `completed`   | archive          | date it shipped                                     |
| `tags`        | TODO             | freeform string list                                |

## Levels — task, scope, epic

`level` says **how big the item is and therefore what "working" it means**. It is
three-tiered, smallest first:

- **`task`** (default) — atomic and implementable as written. Just build it.
- **`scope`** — not yet implementable. Working it means **investigating the code**
  to find out what actually needs to happen, then running `add` for the concrete
  atomic `task`s it breaks into. A scope item produces **todos, not code**; close
  it with `done` once its children exist.
- **`epic`** — a large, spanning feature. It will never have hyper-defined atomic
  steps up front. When you encounter an epic, **do not try to implement the whole
  thing**: scope out the next steps and `add` child items — `scope` items where
  more investigation is needed, `task` items where the work is already clear. An
  epic usually stays active across several rounds of this, shrinking as its
  children ship.

The rule of thumb: **an epic spawns scopes, a scope spawns tasks, a task spawns
code.** When the user says "add this epic," they are asking for the big
definition to be recorded — the breakdown happens later, when it's picked up.

Set it with `add --level scope|epic` or `edit <id> --level …`; filter with
`list --level epic` and tally with `count --by level`. Tasks written before
`level` existed count as `task`.

Active tasks list most-urgent-first, then by `order`. The archive is
newest-`completed`-first.

### Project-specific per-task flags (`store.FLAGS`)

Projects often want a boolean *hint* on each task — "does this one need a full
rebuild?", "does this one need a deploy to staging?", "is this behind a feature
flag?". Add the field names to **`FLAGS`** in
`.claude/skills/todo/scripts/store.py` and the whole CLI picks them up with no
other edits: each gets a `--flag / --no-flag` option on `add` and `edit`
(default **yes** on `add`), shows up in `list`/`show`, is validated as a
boolean, and is dropped when the task is archived (like `urgency`/`order`).

```python
FLAGS = ("rebuild", "deploy_preview")   # → --rebuild/--no-rebuild, --deploy-preview/--no-deploy-preview
```

Document what each flag *means for whoever works the task* in `CLAUDE.md`, so
they're load-bearing rather than decorative. Leave `FLAGS = ()` if the project
doesn't need any.

## Commands

- **`list`** `[--finished] [--status S] [--category C] [--level L] [--json]` — list tasks.
- **`show <id>`** `[--json]` — print one task with its full description.
- **`stats`** `[--json]` — totals plus counts by status, category, urgency, and level
  (active) and by category (finished). This is the "how many …" answer.
- **`count`** `[--finished] [--by status|category|urgency|level]` — a raw count, or
  a grouped tally.
- **`add --title T --description D`** `[--category C] [--urgency U] [--level L]
  [--status S] [--tag t …] [--id ID] [--<flag>/--no-<flag> …]` — append an
  active task. The `id` is a slug of the title (made unique) and `order`
  auto-increments unless given. `urgency` defaults to normal, `level` to `task`;
  any `store.FLAGS` default to yes.
- **`edit <id>`** `[--title|--description|--status|--level|--category|--urgency|--order
  …] [--add-tag t] [--<flag>/--no-<flag> …]` — change fields on an active task.
- **`done <id>`** `[--date YYYY-MM-DD]` — move an active task into
  `FINISHED.toml`, stamped `completed` (today unless `--date`), newest-first.
- **`remove <id>`** `[--reason "…"]` — drop an active task (e.g. descoped).
- **`validate`** — lint both files: required fields, unique ids, valid enum
  values, archive tasks have a `completed` date. Exits non-zero on any problem.

## Working rules (mirror of `CLAUDE.md`)

- When you finish a task (built, tested, documented), run **`done <id>`** in the
  same commit that completes the work — don't leave shipped items in `TODO.toml`.
- **After every `done <id>`, report back two things**: how many active tasks are
  left (`count`), and a one-sentence summary of the very next task (the top item
  from `list`) so the user always knows what's up next without asking.
- When you notice the next thing to build, **`add`** it rather than losing it.
- Prefer the CLI over hand-editing so metadata and ordering stay consistent; if
  you do hand-edit, run **`validate`** afterward.
