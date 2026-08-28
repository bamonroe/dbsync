# Tools & build pipelines

Authoritative home for **external tooling** this project depends on — build pipelines, deploy
scripts, device/emulator harnesses, code generators, and anything else that isn't part of the
repo's own in-tree build. `CLAUDE.md` only *points* here; the specifics live in this file (or
in a dedicated `docs/<tool>.md` that this file links when the detail is large).

Add one entry per tool. Keep each entry to what a Claude instance needs to *use* it: where it
lives, how to invoke it, and the non-obvious gotchas.

## Index

| Tool / pipeline | What it's for | Detail |
|-----------------|---------------|--------|
| Android APK build + emulator | Build APKs and tap-test them headlessly, without a host JDK/SDK/Gradle | [below](#android-apk-build--emulator-datandroid) |

---

## Android APK build + emulator (`/data/android`)

If this project ships an Android app, **don't build it against the host toolchain** — a full
containerized APK build pipeline already exists at **`/data/android`**. It is one front door
for both building and running Android apps: a disposable JDK 21 + SDK builder container, and a
headless Android 14 emulator you drive over `adb`.

- **Where it lives:** `/data/android` (read its `README.md` and `CLAUDE.md` before using it)
- **How to run it:**
  - `./build.sh <project-dir> [gradle-task]` — build an APK (default `:app:assembleDebug`)
  - `.claude/skills/android-dev/scripts/emulator.sh up|status` — start the emulator and wait
    for boot
- **Gotchas:**
  - Machine-specific paths and device IPs live in `config.yaml` (git-ignored) — copy
    `config.example.yaml` first.
  - The emulator needs KVM, so Intel VT-x must be enabled in BIOS.
  - Successful builds auto-publish the APK to the BAM Store; set `BAM_STORE_PUBLISH=0` for a
    local-only build.

## [Tool name]

*Delete this example and add real tools as the project grows.*

- **Where it lives:** `[path or repo]`
- **How to run it:** `[command]`
- **Gotchas:** `[the things that bite — stale caches, required clean builds, auth, etc.]`
