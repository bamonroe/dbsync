//! Translating Dropbox paths into local paths, safely.
//!
//! Everything Dropbox sends us is untrusted input that ends up as a filesystem
//! path, so this is a security boundary: a path that would escape the sync root
//! is rejected outright rather than clamped, because a silent clamp would write
//! the wrong file.

use std::path::{Component, Path, PathBuf};

use crate::error::{Error, Result};

/// Maps between the Dropbox namespace and the local sync root.
#[derive(Debug, Clone)]
pub struct PathMapper {
    local_root: PathBuf,
    /// The Dropbox folder being mirrored, normalised: either empty (the app
    /// root) or a leading-slash path with no trailing slash.
    remote_root: String,
}

impl PathMapper {
    pub fn new(local_root: impl Into<PathBuf>, remote_root: &str) -> Self {
        Self {
            local_root: local_root.into(),
            remote_root: normalise_remote(remote_root),
        }
    }

    pub fn local_root(&self) -> &Path {
        &self.local_root
    }

    /// The Dropbox path to list and long-poll on.
    pub fn remote_root(&self) -> &str {
        &self.remote_root
    }

    /// Where a remote display path belongs on disk.
    ///
    /// Rejects anything outside the mirrored folder, and any component that
    /// could climb out of the local root.
    pub fn to_local(&self, display_path: &str) -> Result<PathBuf> {
        let relative = self.strip_root(display_path)?;
        let mut local = self.local_root.clone();
        for component in Path::new(relative).components() {
            match component {
                Component::Normal(part) => local.push(part),
                // A leading `/` is expected and already consumed by strip_root;
                // anything else here is an escape attempt or a malformed path.
                Component::RootDir | Component::CurDir => {}
                Component::ParentDir | Component::Prefix(_) => {
                    return Err(unsafe_path(display_path));
                }
            }
        }
        Ok(local)
    }

    /// The Dropbox path for a file inside the local root.
    pub fn to_remote(&self, local_path: &Path) -> Result<String> {
        let relative = local_path
            .strip_prefix(&self.local_root)
            .map_err(|_| unsafe_path(&local_path.to_string_lossy()))?;
        let mut remote = self.remote_root.clone();
        for component in relative.components() {
            match component {
                Component::Normal(part) => {
                    remote.push('/');
                    remote.push_str(&part.to_string_lossy());
                }
                _ => return Err(unsafe_path(&local_path.to_string_lossy())),
            }
        }
        Ok(remote)
    }

    /// Drop the mirrored-folder prefix, case-insensitively — Dropbox is
    /// case-insensitive, so the display path's case may differ from ours.
    fn strip_root<'a>(&self, display_path: &'a str) -> Result<&'a str> {
        if self.remote_root.is_empty() {
            return Ok(display_path);
        }
        let (prefix, rest) = display_path
            .split_at_checked(self.remote_root.len())
            .ok_or_else(|| outside_root(display_path))?;
        if !prefix.eq_ignore_ascii_case(&self.remote_root) {
            return Err(outside_root(display_path));
        }
        // "/Work" must not match "/Workshop/a.txt".
        match rest.is_empty() || rest.starts_with('/') {
            true => Ok(rest),
            false => Err(outside_root(display_path)),
        }
    }
}

/// Normalise a configured remote root: `""`, `"/"`, `"Work/"` all mean the same
/// things they look like they mean.
fn normalise_remote(remote_root: &str) -> String {
    let trimmed = remote_root.trim().trim_end_matches('/');
    match trimmed.is_empty() {
        true => String::new(),
        false => match trimmed.starts_with('/') {
            true => trimmed.to_string(),
            false => format!("/{trimmed}"),
        },
    }
}

fn unsafe_path(path: &str) -> Error {
    Error::Config(format!("refusing unsafe path from Dropbox: {path}"))
}

fn outside_root(path: &str) -> Error {
    Error::Config(format!("path is outside the mirrored folder: {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapper(remote_root: &str) -> PathMapper {
        PathMapper::new("/home/me/Dropbox", remote_root)
    }

    #[test]
    fn a_root_mirror_maps_straight_through() {
        assert_eq!(
            mapper("").to_local("/notes/a.txt").unwrap(),
            PathBuf::from("/home/me/Dropbox/notes/a.txt")
        );
    }

    #[test]
    fn a_subfolder_mirror_drops_the_prefix() {
        assert_eq!(
            mapper("/Work").to_local("/Work/notes/a.txt").unwrap(),
            PathBuf::from("/home/me/Dropbox/notes/a.txt")
        );
    }

    /// Dropbox may echo the folder back with different casing; that is the same
    /// folder, not a foreign one.
    #[test]
    fn the_prefix_match_ignores_case() {
        assert!(mapper("/Work").to_local("/work/a.txt").is_ok());
    }

    /// A prefix match must respect path boundaries.
    #[test]
    fn a_sibling_folder_with_a_shared_prefix_is_rejected() {
        assert!(mapper("/Work").to_local("/Workshop/a.txt").is_err());
    }

    /// The security case: a `..` must never be allowed to walk out of the root.
    #[test]
    fn traversal_is_refused_not_clamped() {
        assert!(mapper("").to_local("/../../etc/passwd").is_err());
        assert!(mapper("").to_local("/notes/../../../etc/passwd").is_err());
    }

    #[test]
    fn the_mirrored_folder_itself_maps_to_the_local_root() {
        assert_eq!(
            mapper("/Work").to_local("/Work").unwrap(),
            PathBuf::from("/home/me/Dropbox")
        );
    }

    #[test]
    fn configured_roots_are_normalised() {
        assert_eq!(mapper("Work/").remote_root(), "/Work");
        assert_eq!(mapper("/").remote_root(), "");
        assert_eq!(mapper("  ").remote_root(), "");
    }

    #[test]
    fn a_local_path_maps_back_to_dropbox() {
        assert_eq!(
            mapper("/Work")
                .to_remote(Path::new("/home/me/Dropbox/notes/a.txt"))
                .unwrap(),
            "/Work/notes/a.txt"
        );
        assert_eq!(
            mapper("")
                .to_remote(Path::new("/home/me/Dropbox/a.txt"))
                .unwrap(),
            "/a.txt"
        );
    }

    #[test]
    fn a_local_path_outside_the_root_has_no_remote() {
        assert!(mapper("").to_remote(Path::new("/etc/passwd")).is_err());
    }

    /// Round-tripping must be stable, or an upload could land somewhere other
    /// than where the same file was downloaded from.
    #[test]
    fn local_and_remote_round_trip() {
        let mapper = mapper("/Work");
        let local = mapper.to_local("/Work/a/b.txt").unwrap();
        assert_eq!(mapper.to_remote(&local).unwrap(), "/Work/a/b.txt");
    }
}
