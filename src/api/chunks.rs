//! Turning a file's size into the byte ranges to fetch for it.
//!
//! Pure arithmetic: no network, no disk, no configuration lookups — the same
//! shape as [`crate::reconcile::budget`], and testable the same way. Given a
//! size and the limits, it answers how many chunks and which bytes each one
//! covers, and nothing else.
//!
//! Chunks are a **fixed size**, deliberately. A plan of arbitrary extents
//! would have to be stored to be resumed; with a fixed size, chunk N's offset
//! is `N * chunk_size` and the whole progress record collapses to one bit per
//! chunk. Only the last chunk is short, and its length follows from the size.
//!
//! Two limits keep the split sensible at both ends. Below `min_size` a file is
//! fetched whole, because splitting a 200 KB file four ways pays four round
//! trips to save nothing. Above `max_chunks` the chunk size grows instead of
//! the chunk count, so a 100 GB file yields a manageable plan rather than tens
//! of thousands of ranges and a bitmap to match.

use super::range::ByteRange;

/// What one file is allowed to spend on itself.
///
/// `size` is the revision's length; `budgeted` is how many of those bytes the
/// caller's admission control actually reserved for it. The second is what
/// bounds chunk concurrency, and that is the whole point: a file spends its
/// *own* reservation on its own chunks rather than drawing on a second,
/// independent pool. Two pools would multiply — sixteen files times eight
/// chunks is a hundred and twenty-eight sockets from limits that each looked
/// modest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Allowance {
    /// The revision's total length.
    pub size: u64,
    /// How many of those bytes the caller's budget admitted.
    pub budgeted: u64,
}

/// However large the reservation, one file opening more sockets than this buys
/// throughput back in rate limits.
const MAX_CHUNK_SLOTS: usize = 8;

impl Allowance {
    /// The whole file, unbudgeted — for callers with no admission control of
    /// their own, such as tests and one-shot fetches.
    pub fn whole(size: u64) -> Self {
        Self {
            size,
            budgeted: size,
        }
    }

    /// How many chunks of `chunk_size` may be in flight at once.
    ///
    /// Always at least one: a reservation smaller than a chunk still has to
    /// make progress, it just makes it one chunk at a time.
    pub(super) fn chunk_slots(self, chunk_size: u64) -> usize {
        let fits = self.budgeted / chunk_size.max(1);
        (fits as usize).clamp(1, MAX_CHUNK_SLOTS)
    }
}

/// The limits a plan is built under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Chunking {
    /// Files smaller than this are fetched whole, in one request.
    pub min_size: u64,
    /// The nominal length of every chunk but the last.
    pub chunk_size: u64,
    /// The most chunks one file may be split into.
    pub max_chunks: u32,
}

/// Splitting below this buys nothing: the round trips cost more than the
/// parallelism saves.
pub(super) const DEFAULT_MIN_SIZE: u64 = 8 * 1024 * 1024;
/// Big enough that a chunk is a meaningful amount of streaming, small enough
/// that losing one to an error is cheap to refetch.
pub(super) const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024;
/// Beyond this the plan and its bitmap grow faster than the parallelism helps.
pub(super) const DEFAULT_MAX_CHUNKS: u32 = 64;

impl Default for Chunking {
    fn default() -> Self {
        Self {
            min_size: DEFAULT_MIN_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_chunks: DEFAULT_MAX_CHUNKS,
        }
    }
}

/// How one file is split: a chunk size, a count, and the total they cover.
///
/// A plan always has at least one chunk, even for an empty file — "fetch
/// nothing" is not a case the download path should have to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChunkPlan {
    size: u64,
    chunk_size: u64,
    count: u32,
}

impl ChunkPlan {
    /// Plan the fetch of a `size`-byte file under `limits`.
    pub(super) fn new(size: u64, limits: Chunking) -> Self {
        let chunk_size = limits.chunk_size.max(1);
        let max_chunks = u64::from(limits.max_chunks.max(1));
        if size < limits.min_size.max(1) || size <= chunk_size {
            return Self::whole(size);
        }
        // Past the cap, grow the chunks rather than the count: the alternative
        // is refusing to plan a large file at all.
        let chunk_size = chunk_size.max(size.div_ceil(max_chunks));
        let count = size.div_ceil(chunk_size);
        Self {
            size,
            chunk_size,
            // `count` is bounded by `max_chunks` above, so this cannot truncate.
            count: count as u32,
        }
    }

    /// The single-request plan: one chunk covering everything.
    fn whole(size: u64) -> Self {
        Self {
            size,
            chunk_size: size.max(1),
            count: 1,
        }
    }

    /// Is this a plain whole-file fetch rather than a split one?
    pub(super) fn is_whole_file(self) -> bool {
        self.count == 1
    }

    /// The total length of the file this plan covers.
    pub(super) fn size(self) -> u64 {
        self.size
    }

    /// How many chunks this file is split into — the width of its bitmap.
    pub(super) fn count(self) -> u32 {
        self.count
    }

    /// The nominal chunk length. Every chunk but the last is exactly this.
    pub(super) fn chunk_size(self) -> u64 {
        self.chunk_size
    }

    /// The bytes chunk `index` covers.
    ///
    /// An unsplit file yields the open-ended range, so a whole-file fetch
    /// stays the request it always was rather than becoming a ranged one.
    pub(super) fn range(self, index: u32) -> Option<ByteRange> {
        if index >= self.count {
            return None;
        }
        if self.is_whole_file() {
            return Some(ByteRange::from(0));
        }
        let start = u64::from(index) * self.chunk_size;
        Some(ByteRange::bounded(
            start,
            // Only the last chunk is short, and only when the size is not a
            // whole number of chunks.
            self.chunk_size.min(self.size - start),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every chunk's range, in order — how the fetch loop walks a plan, with
    /// the indices it skips already dropped.
    fn ranges(plan: ChunkPlan) -> Vec<ByteRange> {
        (0..plan.count())
            .filter_map(|index| plan.range(index))
            .collect()
    }

    /// Small enough limits to write the interesting cases by hand.
    fn limits() -> Chunking {
        Chunking {
            min_size: 100,
            chunk_size: 100,
            max_chunks: 4,
        }
    }

    /// An empty file still has to be fetched — the request is what creates the
    /// local file — so a plan is never empty.
    #[test]
    fn an_empty_file_is_still_one_whole_fetch() {
        let plan = ChunkPlan::new(0, limits());
        assert!(plan.is_whole_file());
        assert_eq!(plan.count(), 1);
        assert_eq!(ranges(plan).len(), 1);
    }

    /// The whole point of the threshold: a small file keeps its single request
    /// instead of paying several round trips to save nothing.
    #[test]
    fn a_file_below_the_threshold_is_not_split() {
        assert!(ChunkPlan::new(99, limits()).is_whole_file());
    }

    #[test]
    fn a_file_of_exactly_one_chunk_is_not_split() {
        assert!(ChunkPlan::new(100, limits()).is_whole_file());
    }

    /// A whole-file plan asks open-endedly, so the common download is byte for
    /// byte the request it was before chunking existed.
    #[test]
    fn an_unsplit_plan_asks_for_the_whole_file() {
        let plan = ChunkPlan::new(50, limits());
        assert_eq!(plan.range(0), Some(ByteRange::from(0)));
        assert_eq!(plan.range(1), None);
    }

    #[test]
    fn an_exact_multiple_splits_evenly() {
        let plan = ChunkPlan::new(300, limits());
        assert_eq!(plan.count(), 3);
        assert_eq!(
            ranges(plan),
            vec![
                ByteRange::bounded(0, 100),
                ByteRange::bounded(100, 100),
                ByteRange::bounded(200, 100),
            ]
        );
    }

    /// The last chunk is the only short one; getting its length wrong either
    /// truncates the file or asks past its end for a 416.
    #[test]
    fn the_final_chunk_is_short() {
        let plan = ChunkPlan::new(250, limits());
        assert_eq!(plan.count(), 3);
        assert_eq!(plan.range(2), Some(ByteRange::bounded(200, 50)));
    }

    /// Past the cap the chunks grow instead of the count, or a huge file would
    /// produce thousands of ranges and a bitmap to match.
    #[test]
    fn a_large_file_grows_its_chunks_rather_than_their_number() {
        let plan = ChunkPlan::new(10_000, limits());
        assert_eq!(plan.count(), 4);
        assert_eq!(plan.chunk_size(), 2_500);
    }

    /// Whatever the size, the plan must cover every byte exactly once — the
    /// property the file's correctness actually rests on.
    #[test]
    fn the_chunks_tile_the_file_with_no_gap_or_overlap() {
        for size in [101_u64, 250, 399, 400, 401, 9_999, 10_000] {
            let plan = ChunkPlan::new(size, limits());
            let covered: u64 = (0..plan.count())
                .map(|index| {
                    let start = u64::from(index) * plan.chunk_size();
                    plan.chunk_size().min(size - start)
                })
                .sum();
            assert_eq!(covered, size, "size {size} was not tiled exactly");
        }
    }

    /// A misconfigured zero must not divide by zero or plan nothing.
    #[test]
    fn degenerate_limits_still_produce_a_usable_plan() {
        let plan = ChunkPlan::new(
            1_000,
            Chunking {
                min_size: 0,
                chunk_size: 0,
                max_chunks: 0,
            },
        );
        assert_eq!(plan.count(), 1);
        assert_eq!(ranges(plan).len(), 1);
    }

    /// The shipped defaults have to leave an ordinary large file genuinely
    /// parallel, or the feature does nothing in practice.
    /// A file gets as many sockets as the bytes it actually reserved, so the
    /// per-file and across-file limits compose instead of multiplying.
    #[test]
    fn chunk_slots_follow_the_bytes_actually_reserved() {
        assert_eq!(
            Allowance {
                size: 1_000,
                budgeted: 400
            }
            .chunk_slots(100),
            4
        );
        assert_eq!(
            Allowance {
                size: 1_000,
                budgeted: 1_000
            }
            .chunk_slots(100),
            8
        );
    }

    /// A reservation smaller than one chunk must still make progress.
    #[test]
    fn a_tiny_reservation_still_gets_one_slot() {
        assert_eq!(
            Allowance {
                size: 1_000,
                budgeted: 10
            }
            .chunk_slots(100),
            1
        );
        // A budget of nothing is still a budget for one chunk at a time.
        assert_eq!(
            Allowance {
                size: 50,
                budgeted: 0
            }
            .chunk_slots(10),
            1
        );
    }

    /// Past the cap more sockets buy throughput back in rate limits.
    #[test]
    fn one_file_cannot_open_unlimited_sockets() {
        let huge = Allowance {
            size: u64::MAX,
            budgeted: u64::MAX,
        };
        assert_eq!(huge.chunk_slots(1), MAX_CHUNK_SLOTS);
    }

    #[test]
    fn the_defaults_split_a_large_file() {
        let plan = ChunkPlan::new(1024 * 1024 * 1024, Chunking::default());
        assert!(!plan.is_whole_file());
        assert!(plan.count() > 1 && plan.count() <= DEFAULT_MAX_CHUNKS);
    }
}
