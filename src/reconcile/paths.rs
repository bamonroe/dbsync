//! Translating Dropbox paths into local paths, safely.
//!
//! Everything Dropbox sends us is untrusted input that ends up as a filesystem
//! path, so this is a security boundary: a path that would escape the sync root
//! is rejected outright rather than clamped, because a silent clamp would write
//! the wrong file.

use std::path::{Component, Path, PathBuf};

use crate::error::{Error, Result};

/// The most bytes Linux allows in one path component. Dropbox allows more, so a
/// legal remote name can have no legal local name.
pub(crate) const MAX_COMPONENT_BYTES: usize = 255;

/// How many hex characters of the name's hash are kept when shortening.
///
/// Eight is enough that two names in one folder colliding is not a practical
/// concern, and short enough to leave the readable prefix doing the work.
const FINGERPRINT_HEX: usize = 8;

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
        self.map_local(display_path, true)
    }

    /// The same, without shortening over-long names. Only useful for asking
    /// whether a path *would* have been shortened.
    pub fn to_local_unshortened(&self, display_path: &str) -> Result<PathBuf> {
        self.map_local(display_path, false)
    }

    fn map_local(&self, display_path: &str, shorten_names: bool) -> Result<PathBuf> {
        let relative = self.strip_root(display_path)?;
        let mut local = self.local_root.clone();
        for component in Path::new(relative).components() {
            match component {
                Component::Normal(part) => {
                    let name = part.to_string_lossy();
                    match shorten_names {
                        true => local.push(shorten(&name)),
                        false => local.push(name.as_ref()),
                    }
                }
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

    /// The local path's location relative to the root, as a lookup key.
    ///
    /// Used to find the remote path of a file whose name had to be shortened,
    /// where the on-disk name no longer says what the remote one was.
    pub fn relative_key(&self, local_path: &Path) -> Result<String> {
        let relative = local_path
            .strip_prefix(&self.local_root)
            .map_err(|_| unsafe_path(&local_path.to_string_lossy()))?;
        Ok(relative.to_string_lossy().to_lowercase())
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

/// Shorten one path component to something the filesystem will accept.
///
/// Truncating alone would be wrong twice over: two long names sharing a prefix
/// would collide onto one file, and the result would not be stable if the name
/// changed later in the string. So the kept prefix is followed by a fingerprint
/// of the *whole* original name, which makes the result both collision-resistant
/// and deterministic — the same remote name always lands on the same local one.
///
/// The extension is preserved, because it is what decides whether anything can
/// open the file.
fn shorten(component: &str) -> String {
    shorten_to(component, MAX_COMPONENT_BYTES)
}

/// [`shorten`], but against a caller-supplied limit.
///
/// A download writes to a scratch sibling whose name carries a suffix, so it has
/// less than the full component budget to spend on the name itself; it asks for
/// the smaller limit rather than reserving the space permanently on the real
/// name, which only ever has to fit on its own.
pub(crate) fn shorten_to(component: &str, limit: usize) -> String {
    if component.len() <= limit {
        return component.to_string();
    }
    let fingerprint = fingerprint(component);
    let extension = extension_of(component);
    // "~", the fingerprint, and the extension all have to fit inside the limit.
    let room = limit.saturating_sub(1 + FINGERPRINT_HEX + extension.len());
    let mut prefix = String::new();
    // By characters, not bytes: truncating mid-character would not be valid
    // UTF-8, and these names are full of them.
    for c in component.chars() {
        if prefix.len() + c.len_utf8() > room {
            break;
        }
        prefix.push(c);
    }
    format!("{prefix}~{fingerprint}{extension}")
}

/// The first [`FINGERPRINT_HEX`] hex characters of the name's SHA-256.
fn fingerprint(component: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(component.as_bytes());
    let mut hex = String::with_capacity(FINGERPRINT_HEX);
    for byte in digest.iter() {
        if hex.len() >= FINGERPRINT_HEX {
            break;
        }
        hex.push_str(&format!("{byte:02x}"));
    }
    hex.truncate(FINGERPRINT_HEX);
    hex
}

/// The trailing extension including its dot, if there is a plausible one.
///
/// Bounded deliberately: a "extension" longer than this is not one, it is a
/// name with a dot in it, and keeping it would eat the readable prefix.
fn extension_of(component: &str) -> &str {
    const MOST: usize = 16;
    match component.rfind('.') {
        Some(dot) if component.len() - dot <= MOST && dot > 0 => &component[dot..],
        _ => "",
    }
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

    /// Dropbox allows names longer than Linux does, so a legal remote path can
    /// have no legal local name. It is shortened rather than refused.
    #[test]
    fn an_over_long_name_is_shortened_to_fit() {
        let paths = PathMapper::new("/tmp/root", "");
        let long = "x".repeat(400);
        let local = paths.to_local(&format!("/books/{long}.pdf")).unwrap();

        let name = local.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.len() <= MAX_COMPONENT_BYTES, "{}", name.len());
        assert!(
            name.ends_with(".pdf"),
            "the extension decides what can open it"
        );
        assert!(name.starts_with("xxx"), "the readable prefix is kept");
    }

    /// The same remote name must always land on the same local one, or a second
    /// pull would download it again under a different name.
    #[test]
    fn shortening_is_deterministic() {
        let paths = PathMapper::new("/tmp/root", "");
        let long = format!("/{}.pdf", "y".repeat(400));
        assert_eq!(
            paths.to_local(&long).unwrap(),
            paths.to_local(&long).unwrap()
        );
    }

    /// Truncation alone would map two names sharing a long prefix onto one
    /// file, silently losing one of them.
    #[test]
    fn two_names_sharing_a_long_prefix_do_not_collide() {
        let paths = PathMapper::new("/tmp/root", "");
        let prefix = "z".repeat(400);
        let one = paths.to_local(&format!("/{prefix}one.pdf")).unwrap();
        let two = paths.to_local(&format!("/{prefix}two.pdf")).unwrap();
        assert_ne!(one, two);
    }

    /// Almost every name is under the limit and must pass through untouched.
    #[test]
    fn an_ordinary_name_is_left_alone() {
        let paths = PathMapper::new("/tmp/root", "");
        assert_eq!(
            paths.to_local("/notes/today.md").unwrap(),
            Path::new("/tmp/root/notes/today.md")
        );
    }

    /// Truncating on a byte boundary could split a character in half; these
    /// names are full of them.
    #[test]
    fn shortening_does_not_split_a_multibyte_character() {
        let paths = PathMapper::new("/tmp/root", "");
        let local = paths
            .to_local(&format!("/{}.pdf", "é".repeat(300)))
            .unwrap();
        // Reaching here at all means the name was valid UTF-8; the length check
        // is what proves it actually fits.
        let name = local.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.len() <= MAX_COMPONENT_BYTES);
    }
}
