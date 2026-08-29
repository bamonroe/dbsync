//! The reconciler: the single point where changes are applied.
//!
//! Both directions funnel through here so a path is never written from the
//! remote and local sides at once, and so conflicts produce a
//! `filename (conflicted copy).ext` rather than destroying either version.
//!
//! [`Reconciler`] owns both directions and the state database between them:
//!
//! - [`Reconciler::pull`] answers a [`crate::notify::RemoteEvent::Changed`] by
//!   draining `list_folder/continue`, applying every entry to disk, and
//!   persisting the new cursor.
//! - [`Reconciler::push`] answers a batch from [`crate::watcher`] by uploading
//!   or deleting each local path that actually changed.
//!
//! Three rules from `docs/architecture.md` are enforced here:
//!
//! - **The cursor is only advanced after the page it describes has been
//!   applied**, so a crash re-delivers work rather than skipping it.
//! - **A cursor reset is routine**: drop the cursor, re-list, and reconcile
//!   against local state instead of re-downloading the world.
//! - **The state breaks the echo loop**: a file we just downloaded matches the
//!   state, so the watcher event it caused does not become an upload.

mod apply;
pub mod budget;
mod conflict;
mod local;
mod page;
mod paths;
pub mod schedule;
mod sink;
mod source;
pub mod sweep;
#[cfg(test)]
pub(crate) mod testing;

pub use apply::Applied;
pub use budget::{Admission, Budget};
pub use conflict::conflicted_path;
pub use local::Pushed;
pub use paths::PathMapper;
pub use schedule::{Step, partition};
pub use sink::RemoteSink;
pub use source::RemoteSource;

use std::collections::HashSet;

use crate::api::ListFolderPage;
use crate::error::{Error, Result};
use crate::state::{StateDb, SyncState, key_for};

/// What one pull did. Reported so the daemon can log it and the tests can pin
/// the re-list path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Pull {
    /// How many entries were applied.
    pub applied: usize,
    /// Whether the cursor had to be rebuilt from a full listing.
    pub resynced: bool,
}

/// What one push did, tallied across a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Push {
    /// How many files were uploaded.
    pub uploaded: usize,
    /// How many remote copies were deleted.
    pub deleted: usize,
    /// How many paths Dropbox refused, and so became conflicted copies.
    pub conflicted: usize,
}

/// Applies changes in both directions, and owns the state between them.
pub struct Reconciler<S> {
    source: S,
    paths: PathMapper,
    db: StateDb,
    state: SyncState,
    /// The gate every concurrent download passes through.
    admission: Admission,
}

impl<S: RemoteSource + RemoteSink + Sync> Reconciler<S> {
    pub fn new(source: S, paths: PathMapper, db: StateDb, state: SyncState) -> Self {
        Self::with_budget(source, paths, db, state, Budget::default())
    }

    /// The same, with the download budget chosen rather than defaulted.
    pub fn with_budget(
        source: S,
        paths: PathMapper,
        db: StateDb,
        state: SyncState,
        budget: Budget,
    ) -> Self {
        Self {
            source,
            paths,
            db,
            state,
            admission: Admission::new(budget),
        }
    }

    /// The cursor to long-poll on, once there is one.
    pub fn cursor(&self) -> Option<&str> {
        self.state.cursor()
    }

    pub fn state(&self) -> &SyncState {
        &self.state
    }

    /// Apply everything the remote has for us.
    ///
    /// With no cursor — a first run — this is a full listing. A cursor Dropbox
    /// has invalidated is handled the same way, transparently.
    pub async fn pull(&mut self) -> Result<Pull> {
        let Some(cursor) = self.state.cursor().map(str::to_string) else {
            return self.resync().await;
        };
        match self.pull_from(cursor).await {
            Err(Error::CursorReset) => {
                tracing::info!("cursor reset by Dropbox; re-listing");
                self.state.clear_cursor();
                self.resync().await
            }
            other => other,
        }
    }

    /// Drain `list_folder/continue` from `cursor` until there is no more.
    async fn pull_from(&mut self, cursor: String) -> Result<Pull> {
        let mut pull = Pull::default();
        let mut cursor = cursor;
        loop {
            let page = self.source.list_folder_continue(&cursor).await?;
            let has_more = page.has_more;
            cursor = page.cursor.clone();
            pull.applied += self.apply_page(page).await?;
            if !has_more {
                return Ok(pull);
            }
        }
    }

    /// Rebuild our position from a full listing, reconciling against state.
    ///
    /// Local files the listing does not mention were deleted remotely while we
    /// were not watching, so they go too. Existing entries are kept, which is
    /// what makes this a reconcile rather than a re-download: a file whose
    /// `rev` still matches is skipped.
    async fn resync(&mut self) -> Result<Pull> {
        let mut pull = Pull {
            resynced: true,
            ..Pull::default()
        };
        let mut seen = HashSet::new();
        let mut page = self.source.list_folder(self.paths.remote_root()).await?;
        loop {
            let has_more = page.has_more;
            for entry in &page.entries {
                seen.insert(key_for(entry.display_path()));
            }
            pull.applied += self.apply_page(page).await?;
            if !has_more {
                break;
            }
            let cursor = self.state.cursor().unwrap_or_default().to_string();
            page = self.source.list_folder_continue(&cursor).await?;
        }
        pull.applied += self.drop_vanished(&seen).await?;
        self.db.save(&self.state)?;
        Ok(pull)
    }

    /// Apply a page, then advance and persist the cursor.
    ///
    /// The order matters: advancing first would let a crash mid-page lose
    /// changes permanently, whereas re-applying a page is harmless. Under
    /// concurrency this stops being free — the page's slowest download now
    /// gates the advance — but it is still the right trade, and a Dropbox
    /// cursor is opaque and cannot be positioned mid-page anyway.
    ///
    /// The page itself is applied by [`page::Page`], which overlaps the
    /// downloads it safely can; entries are checkpointed *within* the page as
    /// they complete. A first listing arrives in pages of up to ten thousand
    /// entries, so saving only at the page boundary means an interrupt hours in
    /// loses every entry recorded since the last one — the files are on disk
    /// but untracked, and the next run downloads them all again. Those interim
    /// saves never touch the cursor, so the apply-before-advance ordering above
    /// still holds.
    async fn apply_page(&mut self, page: ListFolderPage) -> Result<usize> {
        let ListFolderPage {
            entries, cursor, ..
        } = page;
        let applied = page::Page {
            source: &self.source,
            paths: &self.paths,
            state: &mut self.state,
            db: &self.db,
            admission: &self.admission,
        }
        .apply(&entries)
        .await?;
        self.state.set_cursor(cursor);
        self.db.save(&self.state)?;
        Ok(applied)
    }

    /// Push a batch of local paths, in the order the watcher settled them.
    ///
    /// One failing path is logged and stepped over: a file that vanished
    /// mid-batch, or one the user cannot read, must not stop the rest.
    pub async fn push(&mut self, batch: &[std::path::PathBuf]) -> Result<Push> {
        let mut push = Push::default();
        let mut changed = false;
        for local in batch {
            match local::push_path(&self.source, &self.paths, &mut self.state, local).await {
                Ok(Pushed::Uploaded) => {
                    push.uploaded += 1;
                    changed = true;
                }
                Ok(Pushed::Deleted) => {
                    push.deleted += 1;
                    changed = true;
                }
                Ok(Pushed::Conflicted) => {
                    push.conflicted += 1;
                    changed = true;
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(path = %local.display(), %error, "could not push local change")
                }
            }
        }
        if changed {
            self.db.save(&self.state)?;
        }
        Ok(push)
    }

    /// Delete local files that the full listing did not mention.
    async fn drop_vanished(&mut self, seen: &HashSet<String>) -> Result<usize> {
        let vanished: Vec<String> = self
            .state
            .entries()
            .map(|entry| entry.display_path.clone())
            .filter(|path| !seen.contains(&key_for(path)))
            .collect();
        let mut removed = 0;
        for path in vanished {
            let local = self.paths.to_local(&path)?;
            match tokio::fs::remove_file(&local).await {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            self.state.remove(&path);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeRemote;
    use super::*;
    use crate::api::{RemoteDeleted, RemoteEntry, RemoteFile};

    fn file(path: &str, rev: &str) -> RemoteEntry {
        RemoteEntry::File(RemoteFile {
            path_lower: path.to_lowercase(),
            path_display: path.to_string(),
            rev: rev.to_string(),
            size: 0,
            content_hash: None,
        })
    }

    fn tombstone(path: &str) -> RemoteEntry {
        RemoteEntry::Deleted(RemoteDeleted {
            path_lower: path.to_lowercase(),
            path_display: Some(path.to_string()),
        })
    }

    fn page(entries: Vec<RemoteEntry>, cursor: &str, has_more: bool) -> Result<ListFolderPage> {
        Ok(ListFolderPage {
            entries,
            cursor: cursor.to_string(),
            has_more,
        })
    }

    struct Fixture {
        dir: tempfile::TempDir,
        applier: Reconciler<FakeRemote>,
    }

    impl Fixture {
        /// An applier over a fresh temp directory, with `cursor` as its
        /// starting position (`None` forces a full listing).
        fn new(cursor: Option<&str>) -> Self {
            Self::with_budget(cursor, Budget::default())
        }

        /// The same, with the download budget chosen.
        fn with_budget(cursor: Option<&str>, budget: Budget) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let mut state = SyncState::new();
            if let Some(cursor) = cursor {
                state.set_cursor(cursor);
            }
            let db = StateDb::at(dir.path().join("state.json"));
            let paths = PathMapper::new(dir.path().join("root"), "");
            std::fs::create_dir_all(dir.path().join("root")).unwrap();
            Self {
                applier: Reconciler::with_budget(FakeRemote::new(), paths, db, state, budget),
                dir,
            }
        }

        fn remote(&self) -> &FakeRemote {
            &self.applier.source
        }

        /// Put `count` distinct files in the fake account and return the
        /// entries a page would carry for them.
        fn stage_files(&self, count: usize) -> Vec<RemoteEntry> {
            (0..count)
                .map(|i| {
                    let path = format!("/f{i}.txt");
                    self.remote().put(&path, path.as_bytes());
                    file(&path, "r1")
                })
                .collect()
        }

        fn local(&self, relative: &str) -> std::path::PathBuf {
            self.dir.path().join("root").join(relative)
        }
    }

    #[tokio::test]
    async fn a_pull_applies_a_page_and_advances_the_cursor() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"hello");
        fixture
            .remote()
            .queue_continue(page(vec![file("/a.txt", "r1")], "c1", false));

        let pull = fixture.applier.pull().await.unwrap();
        assert_eq!(pull.applied, 1);
        assert!(!pull.resynced);
        assert_eq!(fixture.applier.cursor(), Some("c1"));
        assert_eq!(std::fs::read(fixture.local("a.txt")).unwrap(), b"hello");
    }

    /// `has_more` means the change stream is not drained; stopping early would
    /// leave the local copy silently behind.
    #[tokio::test]
    async fn paging_continues_until_has_more_is_false() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"a");
        fixture.remote().put("/b.txt", b"b");
        fixture
            .remote()
            .queue_continue(page(vec![file("/a.txt", "r1")], "c1", true));
        fixture
            .remote()
            .queue_continue(page(vec![file("/b.txt", "r1")], "c2", false));

        let pull = fixture.applier.pull().await.unwrap();
        assert_eq!(pull.applied, 2);
        assert_eq!(fixture.applier.cursor(), Some("c2"));
        // The second page must be fetched with the cursor the first returned.
        assert_eq!(fixture.remote().cursors_used(), vec!["c0", "c1"]);
    }

    /// The cursor is persisted as each page lands, so a restart resumes from
    /// the last fully applied page rather than the start.
    #[tokio::test]
    async fn the_cursor_is_persisted_as_pages_are_applied() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"a");
        fixture
            .remote()
            .queue_continue(page(vec![file("/a.txt", "r1")], "c1", false));
        fixture.applier.pull().await.unwrap();

        let db = StateDb::at(fixture.dir.path().join("state.json"));
        assert_eq!(db.load().unwrap().cursor(), Some("c1"));
    }

    /// A page bigger than the checkpoint interval must persist entries as it
    /// goes. Without interim saves an interrupt part-way through a huge first
    /// listing loses every entry applied so far and re-downloads them all.
    ///
    /// The page here completes out of order — the listing's first file is held
    /// back until every other has landed — which is the case the two halves of
    /// checkpointing have to be driven differently for. The interval counts
    /// *completions*, which under concurrency is no longer a prefix of the
    /// page; the cursor waits for the page barrier instead.
    #[tokio::test]
    async fn entries_are_checkpointed_within_a_long_page() {
        let count = page::CHECKPOINT_EVERY + 5;
        let mut fixture = Fixture::new(Some("c0"));
        let entries = fixture.stage_files(count);
        fixture.remote().stall("/f0.txt", count);
        fixture.remote().queue_continue(page(entries, "c1", false));

        fixture.applier.pull().await.unwrap();

        assert_eq!(fixture.remote().completed().last().unwrap(), "/f0.txt");
        let saved = StateDb::at(fixture.dir.path().join("state.json"))
            .load()
            .unwrap();
        assert_eq!(saved.len(), count);
        assert_eq!(saved.cursor(), Some("c1"));
    }

    /// The page barrier from the failing side: a page that cannot be applied
    /// through to the end must leave the cursor exactly where it was, so the
    /// whole page is re-delivered rather than skipped. Here the mid-page
    /// checkpoint cannot be written, which aborts the page part-way.
    ///
    /// This is the *page* failing, not one entry — a single unapplicable entry
    /// is logged and stepped over, and the cursor does still advance past it
    /// (see `a_failing_entry_does_not_block_the_rest_of_the_page`).
    #[tokio::test]
    async fn a_page_that_cannot_be_finished_leaves_the_cursor_alone() {
        let mut fixture = Fixture::new(Some("c0"));
        let entries = fixture.stage_files(page::CHECKPOINT_EVERY + 5);
        fixture.remote().queue_continue(page(entries, "c1", false));
        // A directory where the state file belongs: every save fails, so the
        // first interim checkpoint takes the page down with it.
        std::fs::create_dir(fixture.dir.path().join("state.json")).unwrap();

        assert!(fixture.applier.pull().await.is_err());
        assert_eq!(fixture.applier.cursor(), Some("c0"));
    }

    /// A page of independent files must overlap its downloads rather than
    /// walking them one at a time — the whole point of the parallel track.
    #[tokio::test]
    async fn a_page_of_files_downloads_more_than_one_at_a_time() {
        let mut fixture = Fixture::new(Some("c0"));
        let entries = fixture.stage_files(8);
        fixture.remote().queue_continue(page(entries, "c1", false));

        let pull = fixture.applier.pull().await.unwrap();
        assert_eq!(pull.applied, 8);
        assert_eq!(fixture.remote().downloads(), 8);
        assert_eq!(fixture.remote().peak_in_flight(), 8);
        assert!(fixture.local("f7.txt").exists());
    }

    /// ...and the budget is what bounds that overlap: a ceiling of one is the
    /// old sequential behaviour, reachable by configuration alone.
    #[tokio::test]
    async fn the_budget_bounds_how_many_overlap() {
        let mut fixture = Fixture::with_budget(
            Some("c0"),
            Budget {
                bytes: 1,
                floor: 1,
                ceiling: 1,
            },
        );
        let entries = fixture.stage_files(6);
        fixture.remote().queue_continue(page(entries, "c1", false));

        fixture.applier.pull().await.unwrap();
        assert_eq!(fixture.remote().downloads(), 6);
        assert_eq!(fixture.remote().peak_in_flight(), 1);
    }

    /// Concurrency must not change what is written: downloads complete out of
    /// order, but outcomes are recorded in listing order, so the state file a
    /// parallel run leaves is byte-identical to a serial one's.
    #[tokio::test]
    async fn a_parallel_page_leaves_the_same_state_as_a_serial_one() {
        async fn run(budget: Budget) -> String {
            let mut fixture = Fixture::with_budget(Some("c0"), budget);
            let entries = fixture.stage_files(12);
            fixture.remote().queue_continue(page(entries, "c1", false));
            fixture.applier.pull().await.unwrap();
            let saved = std::fs::read_to_string(fixture.dir.path().join("state.json")).unwrap();
            // Every mtime is a wall clock reading and differs between runs;
            // everything else must not.
            saved
                .lines()
                .filter(|line| !line.contains("mtime_nanos"))
                .collect()
        }

        let serial = Budget {
            bytes: 1,
            floor: 1,
            ceiling: 1,
        };
        assert_eq!(run(Budget::default()).await, run(serial).await);
    }

    /// One file too big for the whole budget must still be fetched, alone,
    /// rather than waiting for room that will never exist.
    #[tokio::test]
    async fn a_file_larger_than_the_budget_is_still_downloaded() {
        let mut fixture = Fixture::with_budget(
            Some("c0"),
            Budget {
                bytes: 8,
                floor: 1,
                ceiling: 4,
            },
        );
        fixture.remote().put("/big.bin", b"far too many bytes");
        let entry = RemoteEntry::File(RemoteFile {
            path_lower: "/big.bin".into(),
            path_display: "/big.bin".into(),
            rev: "r1".into(),
            size: 1_000_000,
            content_hash: None,
        });
        fixture
            .remote()
            .queue_continue(page(vec![entry], "c1", false));

        assert_eq!(fixture.applier.pull().await.unwrap().applied, 1);
        assert!(fixture.local("big.bin").exists());
    }

    #[tokio::test]
    async fn a_tombstone_in_the_stream_deletes_locally() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"a");
        fixture
            .remote()
            .queue_continue(page(vec![file("/a.txt", "r1")], "c1", false));
        fixture.applier.pull().await.unwrap();

        fixture
            .remote()
            .queue_continue(page(vec![tombstone("/a.txt")], "c2", false));
        fixture.applier.pull().await.unwrap();

        assert!(!fixture.local("a.txt").exists());
        assert!(fixture.applier.state().get("/a.txt").is_none());
    }

    /// With no cursor at all, the first pull is a full listing.
    #[tokio::test]
    async fn a_first_run_lists_the_whole_folder() {
        let mut fixture = Fixture::new(None);
        fixture.remote().put("/a.txt", b"a");
        fixture
            .remote()
            .queue_listing(page(vec![file("/a.txt", "r1")], "c1", false));

        let pull = fixture.applier.pull().await.unwrap();
        assert!(pull.resynced);
        assert_eq!(fixture.applier.cursor(), Some("c1"));
        assert!(fixture.local("a.txt").exists());
    }

    /// The reset path: `continue` fails, and the applier re-lists rather than
    /// giving up.
    #[tokio::test]
    async fn a_cursor_reset_falls_back_to_a_full_listing() {
        let mut fixture = Fixture::new(Some("stale"));
        fixture.remote().put("/a.txt", b"a");
        fixture.remote().queue_continue(Err(Error::CursorReset));
        fixture
            .remote()
            .queue_listing(page(vec![file("/a.txt", "r1")], "fresh", false));

        let pull = fixture.applier.pull().await.unwrap();
        assert!(pull.resynced);
        assert_eq!(fixture.applier.cursor(), Some("fresh"));
        assert!(fixture.local("a.txt").exists());
    }

    /// A re-list must reconcile, not re-download: a file we already hold at the
    /// same revision is left alone.
    #[tokio::test]
    async fn a_re_list_skips_files_we_already_have() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"a");
        fixture
            .remote()
            .queue_continue(page(vec![file("/a.txt", "r1")], "c1", false));
        fixture.applier.pull().await.unwrap();

        fixture.remote().queue_continue(Err(Error::CursorReset));
        fixture
            .remote()
            .queue_listing(page(vec![file("/a.txt", "r1")], "fresh", false));
        fixture.applier.pull().await.unwrap();

        assert_eq!(fixture.remote().downloads(), 1);
    }

    /// A file deleted remotely while the daemon was down leaves no tombstone in
    /// a fresh listing — it is simply absent, and must still be removed.
    #[tokio::test]
    async fn a_re_list_deletes_files_the_remote_no_longer_has() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"a");
        fixture.remote().put("/b.txt", b"b");
        fixture.remote().queue_continue(page(
            vec![file("/a.txt", "r1"), file("/b.txt", "r1")],
            "c1",
            false,
        ));
        fixture.applier.pull().await.unwrap();

        fixture.remote().queue_continue(Err(Error::CursorReset));
        fixture
            .remote()
            .queue_listing(page(vec![file("/a.txt", "r1")], "fresh", false));
        fixture.applier.pull().await.unwrap();

        assert!(fixture.local("a.txt").exists());
        assert!(!fixture.local("b.txt").exists());
        assert!(fixture.applier.state().get("/b.txt").is_none());
    }

    /// The echo loop, end to end: a file we just pulled must not be pushed
    /// straight back up when the watcher notices it landing.
    #[tokio::test]
    async fn a_pulled_file_is_not_pushed_back() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/a.txt", b"a");
        fixture
            .remote()
            .queue_continue(page(vec![file("/a.txt", "r1")], "c1", false));
        fixture.applier.pull().await.unwrap();

        let push = fixture
            .applier
            .push(&[fixture.local("a.txt")])
            .await
            .unwrap();
        assert_eq!(push, Push::default());
        assert_eq!(fixture.remote().uploads(), 0);
    }

    #[tokio::test]
    async fn a_batch_uploads_new_files_and_deletes_removed_ones() {
        let mut fixture = Fixture::new(Some("c0"));
        std::fs::write(fixture.local("new.txt"), b"new").unwrap();
        let push = fixture
            .applier
            .push(&[fixture.local("new.txt")])
            .await
            .unwrap();
        assert_eq!(push.uploaded, 1);

        std::fs::remove_file(fixture.local("new.txt")).unwrap();
        let push = fixture
            .applier
            .push(&[fixture.local("new.txt")])
            .await
            .unwrap();
        assert_eq!(push.deleted, 1);
        assert!(fixture.remote().content("/new.txt").is_none());
    }

    /// A push must persist the state, or a restart would re-upload everything.
    #[tokio::test]
    async fn a_push_persists_the_state() {
        let mut fixture = Fixture::new(Some("c0"));
        std::fs::write(fixture.local("new.txt"), b"new").unwrap();
        fixture
            .applier
            .push(&[fixture.local("new.txt")])
            .await
            .unwrap();

        let db = StateDb::at(fixture.dir.path().join("state.json"));
        assert!(db.load().unwrap().get("/new.txt").is_some());
    }

    /// One bad path in a batch must not stop the ones after it.
    #[tokio::test]
    async fn a_failing_path_does_not_stop_the_batch() {
        let mut fixture = Fixture::new(Some("c0"));
        std::fs::write(fixture.local("good.txt"), b"good").unwrap();
        let batch = vec![
            std::path::PathBuf::from("/etc/passwd"),
            fixture.local("good.txt"),
        ];

        assert_eq!(fixture.applier.push(&batch).await.unwrap().uploaded, 1);
    }

    /// One unapplicable entry must not stall the stream behind it.
    #[tokio::test]
    async fn a_failing_entry_does_not_block_the_rest_of_the_page() {
        let mut fixture = Fixture::new(Some("c0"));
        fixture.remote().put("/good.txt", b"g");
        fixture.remote().queue_continue(page(
            vec![file("/../escape.txt", "r1"), file("/good.txt", "r1")],
            "c1",
            false,
        ));

        let pull = fixture.applier.pull().await.unwrap();
        assert_eq!(pull.applied, 1);
        assert_eq!(fixture.applier.cursor(), Some("c1"));
        assert!(fixture.local("good.txt").exists());
    }
}
