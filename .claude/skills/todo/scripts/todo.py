#!/usr/bin/env python3
"""Command-line front door to the TOML task list (TODO.toml / FINISHED.toml).

Run ``python3 todo.py <command> --help`` for any command. Typical use:

    todo.py list                      # active tasks, most urgent first
    todo.py stats                     # totals by status / category / urgency
    todo.py add --title "..." --description "..." --category feature
    todo.py edit <id> --urgency high
    todo.py done <id>                 # move a task into the archive
    todo.py validate                  # lint both files

All logic lives in store.py / tomlio.py; this file is just argument parsing and
presentation. Project-specific per-task boolean hints come from `store.FLAGS`.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys

import store


# --- helpers ---------------------------------------------------------------

def _today():
    return datetime.date.today()


def _load_todo():
    path = store.todo_path()
    return path, store.load(path)


def _load_finished():
    path = store.finished_path()
    return path, store.load(path)


def _print_task(task, indent=""):
    urg = task.get("urgency", "-")
    cat = task.get("category", "-")
    status = task.get("status", "-")
    print(f"{indent}[{task.get('id','?')}] {task.get('title','')}")
    lvl = task.get("level", store.DEFAULT_LEVEL)
    meta = f"{indent}    level={lvl} status={status} category={cat} urgency={urg}"
    if "order" in task:
        meta += f" order={task['order']}"
    if "completed" in task:
        meta += f" completed={task['completed']}"
    for flag in store.FLAGS:
        if flag in task:
            meta += f" {flag.replace('_', '-')}={'yes' if task[flag] else 'no'}"
    print(meta)


# --- commands --------------------------------------------------------------

def cmd_list(args):
    _, doc = (_load_finished() if args.finished else _load_todo())
    tasks = doc["tasks"]
    if args.status:
        tasks = [t for t in tasks if t.get("status") == args.status]
    if args.category:
        tasks = [t for t in tasks if t.get("category") == args.category]
    if args.level:
        tasks = [t for t in tasks
                 if t.get("level", store.DEFAULT_LEVEL) == args.level]
    if not args.finished:
        tasks = store.sort_active(tasks)
    if args.json:
        print(json.dumps(tasks, default=str, indent=2))
        return
    if not tasks:
        print("(no matching tasks)")
        return
    for task in tasks:
        _print_task(task)


def cmd_show(args):
    for loader in (_load_todo, _load_finished):
        _, doc = loader()
        task = store.find(doc["tasks"], args.id)
        if task:
            if args.json:
                print(json.dumps(task, default=str, indent=2))
            else:
                _print_task(task)
                print(f"\n{task.get('description','')}")
            return
    sys.exit(f"no task with id {args.id!r}")


def cmd_stats(args):
    _, todo = _load_todo()
    _, done = _load_finished()
    active = todo["tasks"]
    payload = {
        "active_total": len(active),
        "finished_total": len(done["tasks"]),
        "active_by_status": store.counts_by(active, "status"),
        "active_by_category": store.counts_by(active, "category"),
        "active_by_urgency": store.counts_by(active, "urgency"),
        "active_by_level": store.counts_by(active, "level"),
        "finished_by_category": store.counts_by(done["tasks"], "category"),
    }
    if args.json:
        print(json.dumps(payload, indent=2))
        return
    print(f"active tasks:   {payload['active_total']}")
    print(f"finished tasks: {payload['finished_total']}")
    for label, key in (
        ("active by status", "active_by_status"),
        ("active by category", "active_by_category"),
        ("active by urgency", "active_by_urgency"),
        ("active by level", "active_by_level"),
        ("finished by category", "finished_by_category"),
    ):
        print(f"\n{label}:")
        for name, n in payload[key].items():
            print(f"  {name:12} {n}")


def cmd_count(args):
    _, doc = (_load_finished() if args.finished else _load_todo())
    tasks = doc["tasks"]
    if args.by:
        for name, n in store.counts_by(tasks, args.by).items():
            print(f"{name:14} {n}")
    else:
        print(len(tasks))


def cmd_add(args):
    path, doc = _load_todo()
    _, done = _load_finished()
    existing = [t.get("id") for t in doc["tasks"] + done["tasks"]]
    task = {
        "id": args.id or store.slugify(args.title, existing),
        "title": args.title,
        "description": args.description,
        "status": args.status,
        "level": args.level,
        "category": args.category,
        "urgency": args.urgency,
        "order": store.next_order(doc["tasks"]),
        "created": _today(),
        "tags": args.tag or [],
    }
    for flag in store.FLAGS:
        task[flag] = getattr(args, flag)
    doc["tasks"].append(task)
    store.save_todo(doc, path)
    print(f"added [{task['id']}] {task['title']}")


def cmd_edit(args):
    path, doc = _load_todo()
    task = store.find(doc["tasks"], args.id)
    if not task:
        sys.exit(f"no active task with id {args.id!r}")
    fields = ("title", "description", "status", "level", "category", "urgency",
              "order") + store.FLAGS
    for field in fields:
        value = getattr(args, field)
        if value is not None:
            task[field] = value
    if args.add_tag:
        task.setdefault("tags", [])
        for tag in args.add_tag:
            if tag not in task["tags"]:
                task["tags"].append(tag)
    store.save_todo(doc, path)
    print(f"updated [{task['id']}]")


def cmd_done(args):
    todo_path, todo = _load_todo()
    fin_path, done = _load_finished()
    task = store.find(todo["tasks"], args.id)
    if not task:
        sys.exit(f"no active task with id {args.id!r}")
    todo["tasks"] = [t for t in todo["tasks"] if t.get("id") != args.id]
    task["status"] = store.STATUS_FINISHED
    task["completed"] = (
        datetime.date.fromisoformat(args.date) if args.date else _today()
    )
    for stray in ("order", "urgency") + store.FLAGS:
        task.pop(stray, None)
    done["tasks"].insert(0, task)  # newest first
    store.save_todo(todo, todo_path)
    store.save_finished(done, fin_path)
    print(f"finished [{task['id']}] on {task['completed']}")


def cmd_remove(args):
    path, doc = _load_todo()
    task = store.find(doc["tasks"], args.id)
    if not task:
        sys.exit(f"no active task with id {args.id!r}")
    doc["tasks"] = [t for t in doc["tasks"] if t.get("id") != args.id]
    store.save_todo(doc, path)
    why = f" ({args.reason})" if args.reason else ""
    print(f"removed [{args.id}]{why}")


def cmd_validate(args):
    problems = []
    seen = {}
    for label, (_, doc), finished in (
        ("TODO", _load_todo(), False),
        ("FINISHED", _load_finished(), True),
    ):
        for task in doc["tasks"]:
            problems += _validate_task(label, task, finished, seen)
    if problems:
        print("\n".join(problems))
        sys.exit(1)
    print("ok — both files validate")


def _validate_task(label, task, finished, seen):
    out = []
    tid = task.get("id")
    where = f"{label} [{tid or '?'}]"
    for field in ("id", "title", "description", "status"):
        if not task.get(field):
            out.append(f"{where}: missing {field}")
    if tid in seen:
        out.append(f"{where}: duplicate id (also in {seen[tid]})")
    elif tid:
        seen[tid] = label
    if "category" in task and task["category"] not in store.CATEGORIES:
        out.append(f"{where}: unknown category {task['category']!r}")
    if "level" in task and task["level"] not in store.LEVELS:
        out.append(f"{where}: unknown level {task['level']!r}")
    if finished:
        if task.get("status") != store.STATUS_FINISHED:
            out.append(f"{where}: archive task not marked finished")
        if not task.get("completed"):
            out.append(f"{where}: missing completed date")
    else:
        if task.get("status") not in store.STATUSES_ACTIVE:
            out.append(f"{where}: status not one of {store.STATUSES_ACTIVE}")
        if task.get("urgency") and task["urgency"] not in store.URGENCIES:
            out.append(f"{where}: unknown urgency {task['urgency']!r}")
        for flag in store.FLAGS:
            if flag in task and not isinstance(task[flag], bool):
                out.append(f"{where}: {flag} must be true or false")
    return out


# --- argument parser -------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description="TOML task-list manager")
    sub = p.add_subparsers(dest="command", required=True)

    ls = sub.add_parser("list", help="list tasks")
    ls.add_argument("--finished", action="store_true", help="list the archive")
    ls.add_argument("--status")
    ls.add_argument("--category")
    ls.add_argument("--level", choices=store.LEVELS)
    ls.add_argument("--json", action="store_true")
    ls.set_defaults(func=cmd_list)

    sh = sub.add_parser("show", help="show one task by id")
    sh.add_argument("id")
    sh.add_argument("--json", action="store_true")
    sh.set_defaults(func=cmd_show)

    st = sub.add_parser("stats", help="summary counts")
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_stats)

    ct = sub.add_parser("count", help="count tasks, optionally grouped")
    ct.add_argument("--finished", action="store_true")
    ct.add_argument("--by", choices=("status", "category", "urgency", "level"))
    ct.set_defaults(func=cmd_count)

    ad = sub.add_parser("add", help="add an active task")
    ad.add_argument("--title", required=True)
    ad.add_argument("--description", required=True)
    ad.add_argument("--level", choices=store.LEVELS, default=store.DEFAULT_LEVEL,
                    help="task (atomic) | scope (investigate, then add tasks) | "
                         "epic (large; break into scope/task children)")
    ad.add_argument("--category", choices=store.CATEGORIES, default="feature")
    ad.add_argument("--urgency", choices=store.URGENCIES, default="normal")
    ad.add_argument("--status", choices=store.STATUSES_ACTIVE, default="active")
    ad.add_argument("--id", help="explicit id (default: slug of title)")
    ad.add_argument("--tag", action="append")
    for flag in store.FLAGS:
        ad.add_argument(f"--{flag.replace('_', '-')}",
                        action=argparse.BooleanOptionalAction, default=True,
                        help=f"per-task hint {flag} (default: yes)")
    ad.set_defaults(func=cmd_add)

    ed = sub.add_parser("edit", help="edit an active task")
    ed.add_argument("id")
    ed.add_argument("--title")
    ed.add_argument("--description")
    ed.add_argument("--status", choices=store.STATUSES_ACTIVE)
    ed.add_argument("--level", choices=store.LEVELS)
    ed.add_argument("--category", choices=store.CATEGORIES)
    ed.add_argument("--urgency", choices=store.URGENCIES)
    ed.add_argument("--order", type=int)
    ed.add_argument("--add-tag", action="append")
    for flag in store.FLAGS:
        ed.add_argument(f"--{flag.replace('_', '-')}",
                        action=argparse.BooleanOptionalAction, default=None,
                        help=f"set the per-task hint {flag}")
    ed.set_defaults(func=cmd_edit)

    dn = sub.add_parser("done", help="move a task to the archive")
    dn.add_argument("id")
    dn.add_argument("--date", help="completion date YYYY-MM-DD (default: today)")
    dn.set_defaults(func=cmd_done)

    rm = sub.add_parser("remove", help="drop an active task")
    rm.add_argument("id")
    rm.add_argument("--reason")
    rm.set_defaults(func=cmd_remove)

    va = sub.add_parser("validate", help="lint both task files")
    va.set_defaults(func=cmd_validate)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
