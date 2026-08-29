//! Fails the build when `config.example.toml` drifts from the `Config` struct.
//!
//! `config.example.toml` is what `README.md` tells operators to copy, so its
//! keys are a code-derived fact: they must mirror `src/config.rs` exactly. Both
//! directions are checked mechanically —
//!
//! * an **extra** key in the example fails to deserialize (`deny_unknown_fields`);
//! * a **missing** key is caught by round-tripping the parsed `Config` back to
//!   TOML and comparing the two sets of dotted key paths.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use dbsync::config::Config;

/// Collect every leaf key as a dotted path (`longpoll.timeout_secs`).
fn key_paths(value: &toml::Value, prefix: &str, out: &mut BTreeSet<String>) {
    let Some(table) = value.as_table() else {
        out.insert(prefix.to_string());
        return;
    };
    for (key, child) in table {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        key_paths(child, &path, out);
    }
}

fn paths_of(text: &str) -> BTreeSet<String> {
    let value: toml::Value = toml::from_str(text).expect("valid TOML");
    let mut out = BTreeSet::new();
    key_paths(&value, "", &mut out);
    out
}

fn example_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml")
}

#[test]
fn example_config_has_exactly_the_keys_the_config_struct_defines() {
    let text = std::fs::read_to_string(example_path()).expect("config.example.toml is readable");

    // Rejects keys the struct doesn't have, thanks to `deny_unknown_fields`.
    let config: Config = toml::from_str(&text).expect(
        "config.example.toml must parse as Config — an unknown key here means the example \
         has drifted ahead of src/config.rs",
    );

    let documented = paths_of(&text);
    let defined = paths_of(&toml::to_string(&config).expect("Config re-serializes"));

    let missing: Vec<_> = defined.difference(&documented).collect();
    assert!(
        missing.is_empty(),
        "config.example.toml is missing keys defined in src/config.rs: {missing:?}",
    );
    // Belt and braces: `deny_unknown_fields` already covers this direction.
    let extra: Vec<_> = documented.difference(&defined).collect();
    assert!(
        extra.is_empty(),
        "config.example.toml documents keys src/config.rs does not define: {extra:?}",
    );
}

#[test]
fn the_shipped_example_is_a_working_starting_point() {
    // It parses and every value is in range; only the placeholder app key
    // stands between an operator and a running daemon.
    let text = std::fs::read_to_string(example_path()).unwrap();
    let filled = text.replace("your-app-key-here", "abc123");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, filled).unwrap();

    let config =
        Config::load(&path).expect("the shipped example must be valid once app_key is set");
    assert_eq!(config.longpoll.timeout_secs, 300);
}
