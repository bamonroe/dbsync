//! Retry requests: the one thing the CLI may ask of a running daemon.
//!
//! `dbsync retry <path>` has an awkward constraint. The daemon owns
//! [`super::SyncState`] and rewrites `state.json` wholesale, so a second
//! process editing that file would simply be overwritten by the daemon's next
//! save — the request would vanish without a trace. There is no IPC channel
//! here and adding one for a single verb is out of proportion.
//!
//! So the request goes in its own file, which only the CLI writes and only the
//! daemon removes. The CLI appends a line; the daemon takes the whole file at
//! the start of its next pass and deletes it. The two never write the same
//! file, so there is nothing to race over.
//!
//! The format is one path per line rather than JSON, because appending to it
//! must not require reading and re-serialising what is already there — a second
//! `dbsync retry` while the daemon is mid-take should lose at most its own line,
//! never someone else's.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::state::Direction;

/// One path an operator asked to be tried again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryRequest {
    /// The remote display path, as the operator typed it.
    pub display_path: String,
    /// Which direction to try. An operator asking for a *download* of a path
    /// whose upload failed would destroy their own edit, so the direction is
    /// theirs to state rather than ours to guess.
    pub direction: Direction,
}

/// The queue of retry requests, as a file beside the state database.
pub struct RetryQueue {
    path: PathBuf,
}

impl RetryQueue {
    pub fn at(path: PathBuf) -> Self {
        Self { path }
    }

    /// The queue belonging to the state database at `state_path`.
    pub fn beside(state_path: &Path) -> Self {
        Self::at(state_path.with_file_name("retry-requests"))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a request. Creates the file if this is the first one.
    pub fn push(&self, request: &RetryRequest) -> Result<()> {
        // Checked before the file is touched: a path containing a newline
        // could not round-trip, and Dropbox does allow one. Rejecting is
        // honest; silently mangling it is not.
        if request.display_path.contains('\n') {
            return Err(Error::Config(
                "a path containing a newline cannot be queued for retry".into(),
            ));
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(
            file,
            "{}\t{}",
            request.direction.label(),
            request.display_path
        )?;
        Ok(())
    }

    /// Take every queued request and clear the queue.
    ///
    /// Removing the file is the acknowledgement: a request is acted on exactly
    /// once, and a daemon that is not running simply leaves the queue for
    /// whenever it next starts.
    pub fn take(&self) -> Result<Vec<RetryRequest>> {
        let Some(text) = crate::fsutil::read_optional(&self.path)? else {
            return Ok(Vec::new());
        };
        // Removed before the lines are handed out: a malformed line must not
        // leave the queue in place to be re-read on every pass forever.
        crate::fsutil::remove_if_present(&self.path)?;
        Ok(text.lines().filter_map(parse_line).collect())
    }
}

/// Parse one `<direction>\t<path>` line, ignoring anything unreadable.
///
/// A line we cannot parse is dropped rather than failing the take: the worst
/// case is one retry that does not happen, against a daemon that refuses to
/// sync at all.
fn parse_line(line: &str) -> Option<RetryRequest> {
    let (direction, path) = line.split_once('\t')?;
    if path.is_empty() {
        return None;
    }
    let direction = Direction::from_label(direction)?;
    Some(RetryRequest {
        display_path: path.to_string(),
        direction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue() -> (tempfile::TempDir, RetryQueue) {
        let dir = tempfile::tempdir().unwrap();
        let queue = RetryQueue::beside(&dir.path().join("state.json"));
        (dir, queue)
    }

    fn request(path: &str, direction: Direction) -> RetryRequest {
        RetryRequest {
            display_path: path.to_string(),
            direction,
        }
    }

    #[test]
    fn requests_survive_the_round_trip_in_order() {
        let (_dir, queue) = queue();
        queue.push(&request("/a.txt", Direction::Download)).unwrap();
        queue.push(&request("/b.txt", Direction::Upload)).unwrap();

        assert_eq!(
            queue.take().unwrap(),
            vec![
                request("/a.txt", Direction::Download),
                request("/b.txt", Direction::Upload),
            ]
        );
    }

    /// Taking is the acknowledgement, so a request is acted on exactly once.
    #[test]
    fn taking_clears_the_queue() {
        let (_dir, queue) = queue();
        queue.push(&request("/a.txt", Direction::Download)).unwrap();

        assert_eq!(queue.take().unwrap().len(), 1);
        assert!(queue.take().unwrap().is_empty());
        assert!(!queue.path().exists());
    }

    /// The common case: nothing has ever been requested.
    #[test]
    fn an_absent_queue_is_empty_rather_than_an_error() {
        let (_dir, queue) = queue();
        assert!(queue.take().unwrap().is_empty());
    }

    /// One unreadable line must not stop the daemon syncing forever.
    #[test]
    fn a_malformed_line_is_dropped_and_the_rest_are_kept() {
        let (_dir, queue) = queue();
        std::fs::write(
            queue.path(),
            "nonsense\nsideways\t/x.txt\ndownload\t/a.txt\n",
        )
        .unwrap();

        assert_eq!(
            queue.take().unwrap(),
            vec![request("/a.txt", Direction::Download)]
        );
    }

    /// Dropbox allows a newline in a path; this file format cannot carry one,
    /// and mangling it silently would queue a retry for the wrong file.
    #[test]
    fn a_path_with_a_newline_is_refused_rather_than_mangled() {
        let (_dir, queue) = queue();
        assert!(
            queue
                .push(&request("/two\nlines.txt", Direction::Download))
                .is_err()
        );
    }
}
