//! Small filesystem helpers shared by everything that keeps a file on disk.
//!
//! Four patterns showed up in the state database, the credential store, the
//! journal, the retry queue and the partial sweep, each written out by hand:
//! resolve the data directory, read a file whose absence is normal, remove a
//! file whose absence is normal, and replace a file atomically. They live here
//! so the durability rules — sync the bytes, rename, then sync the directory —
//! are stated once rather than re-derived per call site.

use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::error::{Error, Result};

/// Owner-only, for a directory holding a file written with a mode.
const PRIVATE_DIR_MODE: u32 = 0o700;

/// `$XDG_DATA_HOME/dbsync`, where everything long-lived is kept.
///
/// In the container `compose.yaml` mounts a named volume here, so state and
/// credentials survive rebuilds and never land in the image.
pub fn data_dir() -> Result<PathBuf> {
    let dirs = directories::ProjectDirs::from("", "", "dbsync")
        .ok_or_else(|| Error::Config("cannot determine a home directory".into()))?;
    Ok(dirs.data_dir().to_path_buf())
}

/// Read `path`, treating "not there" as `None` rather than an error.
///
/// Every file this crate reads is one it may not have written yet, so a missing
/// file is the first-run case and not a failure.
pub fn read_optional(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(Error::ReadFile {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// [`read_optional`] for a reader, when the file is streamed rather than slurped.
pub fn open_optional(path: &Path) -> Result<Option<std::fs::File>> {
    match std::fs::File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(Error::ReadFile {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Delete `path`, treating "already gone" as success. True if it was there.
pub fn remove_if_present(path: &Path) -> Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Serialise `value` and replace `path` with it, atomically.
///
/// `what` names the value for the serialisation error; `mode`, when given, is
/// the permission the temporary file is *created* with — writing first and
/// chmod-ing after would leave a credential world-readable in between — and
/// also makes the parent directory owner-only.
///
/// A crash leaves either the old file or the new one, never a partial one: the
/// bytes are synced before the rename so they are durable, and the directory is
/// synced after so the rename itself survives power loss.
pub fn write_json_atomically<T: Serialize>(
    path: &Path,
    value: &T,
    what: &str,
    mode: Option<u32>,
) -> Result<()> {
    let json = serde_json::to_string_pretty(value)
        .map_err(|e| Error::Config(format!("cannot serialise {what}: {e}")))?;
    write_atomically(path, json.as_bytes(), mode)
}

/// [`write_json_atomically`] for bytes that are already serialised.
pub fn write_atomically(path: &Path, contents: &[u8], mode: Option<u32>) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Config(format!("{} has no parent directory", path.display())))?;
    std::fs::create_dir_all(parent)?;
    if mode.is_some() {
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(PRIVATE_DIR_MODE))?;
    }

    let temp = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        if let Some(mode) = mode {
            options.mode(mode);
        }
        let mut file = options.open(&temp)?;
        file.write_all(contents)?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_file_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        assert!(read_optional(&path).unwrap().is_none());
        assert!(open_optional(&path).unwrap().is_none());
    }

    #[test]
    fn removing_an_absent_file_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!remove_if_present(&dir.path().join("absent")).unwrap());
    }

    #[test]
    fn a_written_file_round_trips_and_is_removable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("value.json");
        write_json_atomically(&path, &vec![1, 2, 3], "numbers", None).unwrap();
        assert_eq!(
            read_optional(&path)
                .unwrap()
                .unwrap()
                .replace(char::is_whitespace, ""),
            "[1,2,3]"
        );
        assert!(remove_if_present(&path).unwrap());
    }

    /// The temporary file must not survive the write.
    #[test]
    fn no_temp_file_survives_a_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("value.json");
        write_atomically(&path, b"x", None).unwrap();
        assert!(!path.with_extension("tmp").exists());
    }

    /// A mode makes both the file and the directory holding it owner-only.
    #[test]
    fn a_mode_is_applied_to_the_file_and_its_directory() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("private");
        let path = parent.join("secret.json");
        write_atomically(&path, b"x", Some(0o600)).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(&parent), PRIVATE_DIR_MODE);
    }

    /// An existing file is replaced, not appended to.
    #[test]
    fn writing_twice_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("value");
        write_atomically(&path, b"first", None).unwrap();
        write_atomically(&path, b"second", None).unwrap();
        assert_eq!(read_optional(&path).unwrap().unwrap(), "second");
    }
}
