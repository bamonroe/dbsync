//! Rebuilding a remote path through the folder casings we have been told.
//!
//! Dropbox returns two forms of every path: `path_lower`, wholly lowercased,
//! and `path_display`, which carries the real capitalisation. The catch is that
//! `path_display` is only dependable for the path's **last** component. Ask for
//! a file and the folders above it may come back lowercased; ask for the folder
//! itself and it is named correctly.
//!
//! That is enough to get right, because a listing hands out a folder before the
//! things inside it. Each folder entry is recorded under its lowercased path
//! ([`SyncState::record_folder_case`]), and every path afterwards is rebuilt
//! component by component through those recordings. Without it, a deep file
//! creates its parents from its own display path and freezes the wrong case
//! onto a filesystem that, unlike Dropbox, will never treat the two as one.

use crate::state::SyncState;

/// `display_path` with every folder above the last component replaced by the
/// casing its own entry gave.
///
/// The final component is left exactly as it came: it is the one part Dropbox
/// capitalises reliably, and for a folder entry it is the authority the rest of
/// this module is built on.
///
/// A folder that has not been seen keeps the case it arrived with, so this is
/// never worse than not calling it.
pub fn canonical(state: &SyncState, display_path: &str) -> String {
    let trimmed = display_path.trim_start_matches('/');
    if trimmed.is_empty() {
        return display_path.to_string();
    }
    let components: Vec<&str> = trimmed.split('/').collect();
    let (last, parents) = components.split_last().expect("checked non-empty");

    let mut rebuilt: Vec<String> = Vec::with_capacity(components.len());
    for parent in parents {
        // Look the folder up by the path built so far, not by its bare name:
        // two folders of the same name in different places are different
        // folders and may well be capitalised differently.
        let mut key = rebuilt.join("/");
        if !key.is_empty() {
            key.push('/');
        }
        key.push_str(parent);
        key = key.to_lowercase();
        match state.folder_case(&key) {
            Some(known) => rebuilt.push(last_component(known).to_string()),
            None => rebuilt.push((*parent).to_string()),
        }
    }
    rebuilt.push((*last).to_string());
    format!("/{}", rebuilt.join("/"))
}

/// The part after the last `/`, or the whole thing when there is none.
fn last_component(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// The path relative to the sync root, as [`SyncState`] keys folders.
pub fn relative(display_path: &str) -> &str {
    display_path.trim_start_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(folders: &[&str]) -> SyncState {
        let mut state = SyncState::new();
        for folder in folders {
            state.record_folder_case(relative(folder));
        }
        state
    }

    #[test]
    fn an_unseen_path_is_returned_as_it_came() {
        let state = SyncState::new();
        assert_eq!(canonical(&state, "/Photos/Cat.jpg"), "/Photos/Cat.jpg");
    }

    /// The case this whole module exists for: Dropbox lowercases the folders
    /// above a file, and the folder's own entry is what puts them back.
    #[test]
    fn a_lowercased_parent_is_restored_from_its_folder_entry() {
        let state = state_with(&["/GWH and Brian", "/GWH and Brian/JRI paper"]);
        assert_eq!(
            canonical(&state, "/GWH and Brian/jri paper/Demographics.xlsx"),
            "/GWH and Brian/JRI paper/Demographics.xlsx"
        );
    }

    /// Every level is rebuilt, not just the one nearest the file.
    #[test]
    fn several_levels_are_restored_at_once() {
        let state = state_with(&["/Papers", "/Papers/Stata", "/Papers/Stata/DoFiles"]);
        assert_eq!(
            canonical(&state, "/papers/stata/dofiles/Supplementary.do"),
            "/Papers/Stata/DoFiles/Supplementary.do"
        );
    }

    /// A folder entry arrives with its own parents lowercased too, so it has to
    /// be canonicalised before it is recorded or the wrong key goes in.
    #[test]
    fn a_folder_records_itself_under_its_rebuilt_parents() {
        let mut state = state_with(&["/Papers"]);
        let rebuilt = canonical(&state, "/papers/Stata");
        assert_eq!(rebuilt, "/Papers/Stata");
        state.record_folder_case(relative(&rebuilt));
        assert_eq!(
            canonical(&state, "/papers/stata/a.do"),
            "/Papers/Stata/a.do"
        );
    }

    /// The last component is Dropbox's to decide, and a file whose own name
    /// happens to match a folder must not be rewritten to the folder's case.
    #[test]
    fn the_final_component_is_left_alone() {
        let state = state_with(&["/Notes", "/Notes/Draft"]);
        assert_eq!(canonical(&state, "/Notes/draft"), "/Notes/draft");
    }

    #[test]
    fn a_root_level_name_is_unchanged() {
        let state = state_with(&["/Photos"]);
        assert_eq!(canonical(&state, "/Photos"), "/Photos");
        assert_eq!(canonical(&state, "/"), "/");
    }

    /// Recording the same casing twice must not queue a second journal record.
    #[test]
    fn recording_an_unchanged_casing_is_a_no_op() {
        let mut state = state_with(&["/Papers"]);
        let before = state.folder_case_count();
        state.record_folder_case("Papers");
        assert_eq!(state.folder_case_count(), before);
    }
}
