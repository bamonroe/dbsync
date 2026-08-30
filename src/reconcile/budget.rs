//! Admission control for parallel downloads, measured in bytes.
//!
//! A count of files is the wrong unit for "how much may be in flight": eight
//! 1 KB files and eight 1 GB files are the same number and nothing like the
//! same load. [`RemoteFile::size`](crate::api::RemoteFile) is known before a
//! byte is fetched, so a download's cost needs no estimating — the budget here
//! is exact.
//!
//! Three limits interact:
//!
//! - a **byte budget** ([`Budget::bytes`]) bounds the total size in flight, so
//!   a hundred tiny files all admit at once while one huge file admits alone;
//! - a **floor** ([`Budget::floor`]) admits that many downloads regardless of
//!   the byte budget, so one enormous file does not serialise every small file
//!   queued behind it;
//! - a **ceiling** ([`Budget::ceiling`]) caps the count whatever the sizes, so
//!   ten thousand 1 KB files do not open ten thousand sockets.
//!
//! The floor and the byte budget genuinely conflict — the floor is by
//! definition permission to exceed the budget — and the floor wins. The
//! ceiling is absolute and beats both.
//!
//! A cost larger than the whole budget is clamped to the budget, so an
//! oversized file is admitted alone rather than being unschedulable forever.
//!
//! This module touches neither the network nor the disk: it is a counter and a
//! condition, and its tests are pure.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// The limits an [`Admission`] enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// How many bytes may be in flight at once, before the floor overrides it.
    pub bytes: u64,
    /// How many downloads are admitted regardless of the byte budget.
    pub floor: usize,
    /// The hard cap on downloads in flight, whatever their size.
    pub ceiling: usize,
}

/// 256 MB in flight: enough that a handful of large files can't starve the
/// rest of the queue, without holding an unbounded amount of traffic in
/// memory and sockets.
pub const DEFAULT_BYTES: u64 = 256 * 1024 * 1024;
/// Small files should still overlap when one big file has eaten the budget.
pub const DEFAULT_FLOOR: usize = 16;
/// Measured, not guessed: a cold sync of a real account moved 1.44 GB and
/// 2289 files in its first 270 s at 48, against 484 MB / 534 files at 16.
/// Doubling again to 96 made it *worse* (1.16 GB / 1420 files) with no rate
/// limiting in the logs, so the loss there is contention, not throttling.
pub const DEFAULT_CEILING: usize = 48;

impl Default for Budget {
    fn default() -> Self {
        Self {
            bytes: DEFAULT_BYTES,
            floor: DEFAULT_FLOOR,
            ceiling: DEFAULT_CEILING,
        }
    }
}

impl Budget {
    /// The limits with a ceiling no lower than the floor and both at least one,
    /// so a misconfigured pair cannot deadlock every download.
    fn sane(self) -> Self {
        let floor = self.floor.max(1);
        Self {
            bytes: self.bytes.max(1),
            floor,
            ceiling: self.ceiling.max(floor),
        }
    }
}

/// What is in flight right now.
#[derive(Debug, Default)]
struct InFlight {
    bytes: u64,
    count: usize,
}

#[derive(Debug)]
struct Inner {
    budget: Budget,
    in_flight: Mutex<InFlight>,
    /// Woken every time a permit is released, so waiters re-test the condition.
    released: Notify,
}

impl Inner {
    /// Whether a download of `cost` may start given what is already in flight.
    ///
    /// The ceiling is absolute; under it, admission is either "the floor says
    /// so" or "the bytes fit". `cost` is already clamped to the budget, so a
    /// lone oversized download always fits an empty budget.
    fn admits(&self, in_flight: &InFlight, cost: u64) -> bool {
        if in_flight.count >= self.budget.ceiling {
            return false;
        }
        in_flight.count < self.budget.floor || in_flight.bytes + cost <= self.budget.bytes
    }

    fn release(&self, cost: u64) {
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            in_flight.bytes -= cost;
            in_flight.count -= 1;
        }
        self.released.notify_waiters();
    }
}

/// Gate through which every parallel download passes before it starts.
///
/// Cheap to clone: every clone shares one budget.
#[derive(Debug, Clone)]
pub struct Admission {
    inner: Arc<Inner>,
}

impl Admission {
    pub fn new(budget: Budget) -> Self {
        Self {
            inner: Arc::new(Inner {
                budget: budget.sane(),
                in_flight: Mutex::new(InFlight::default()),
                released: Notify::new(),
            }),
        }
    }

    /// The limits in force, after sanitising.
    pub fn budget(&self) -> Budget {
        self.inner.budget
    }

    /// What a file of `size` costs, clamped to the whole budget.
    fn cost(&self, size: u64) -> u64 {
        size.min(self.inner.budget.bytes)
    }

    /// Take a slot for a download of `size` bytes, or `None` if one is not
    /// free this instant. Never waits.
    pub fn try_acquire(&self, size: u64) -> Option<Permit> {
        let cost = self.cost(size);
        let mut in_flight = self.inner.in_flight.lock().unwrap();
        if !self.inner.admits(&in_flight, cost) {
            return None;
        }
        in_flight.bytes += cost;
        in_flight.count += 1;
        drop(in_flight);
        Some(Permit {
            inner: Arc::clone(&self.inner),
            cost,
        })
    }

    /// Take a slot for a download of `size` bytes, waiting until one frees.
    pub async fn acquire(&self, size: u64) -> Permit {
        loop {
            // Registered before the check, so a release that lands between the
            // two still wakes us rather than being missed.
            let notified = self.inner.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(permit) = self.try_acquire(size) {
                return permit;
            }
            notified.await;
        }
    }

    /// How many bytes are in flight — for tests and for logging.
    pub fn bytes_in_flight(&self) -> u64 {
        self.inner.in_flight.lock().unwrap().bytes
    }

    /// How many downloads are in flight — for tests and for logging.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.lock().unwrap().count
    }
}

impl Default for Admission {
    fn default() -> Self {
        Self::new(Budget::default())
    }
}

/// A download's claim on the budget, given back when it is dropped.
#[derive(Debug)]
pub struct Permit {
    inner: Arc<Inner>,
    cost: u64,
}

impl Permit {
    /// The bytes this permit reserved, after clamping.
    pub fn cost(&self) -> u64 {
        self.cost
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.inner.release(self.cost);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(bytes: u64, floor: usize, ceiling: usize) -> Admission {
        Admission::new(Budget {
            bytes,
            floor,
            ceiling,
        })
    }

    /// The point of counting bytes: small files do not each burn a slot.
    #[test]
    fn many_small_files_all_admit_at_once() {
        let admission = budget(1_000_000, 1, 100);
        let permits: Vec<_> = (0..50)
            .map(|_| admission.try_acquire(1_000).expect("small file admits"))
            .collect();
        assert_eq!(permits.len(), 50);
        assert_eq!(admission.bytes_in_flight(), 50_000);
    }

    /// ...and the other half: one file the size of the budget runs alone.
    #[test]
    fn a_budget_sized_file_admits_alone() {
        let admission = budget(1_000, 1, 16);
        let _big = admission.try_acquire(1_000).unwrap();
        assert!(admission.try_acquire(1).is_none());
    }

    /// A file bigger than the whole budget must still be downloadable; its cost
    /// is clamped so it is admitted alone rather than never.
    #[test]
    fn an_oversized_file_is_clamped_and_still_admitted() {
        let admission = budget(1_000, 1, 16);
        let permit = admission
            .try_acquire(10_000_000)
            .expect("clamped to budget");
        assert_eq!(permit.cost(), 1_000);
        assert!(admission.try_acquire(1).is_none());
    }

    /// The conflict this design has to resolve: the floor admits past a full
    /// budget on purpose, so a huge file cannot stall everything behind it.
    #[test]
    fn the_floor_admits_past_a_full_budget() {
        let admission = budget(1_000, 3, 16);
        let _big = admission.try_acquire(1_000).unwrap();
        assert!(admission.bytes_in_flight() >= admission.budget().bytes);

        let _second = admission.try_acquire(500).expect("floor beats the budget");
        let _third = admission.try_acquire(500).expect("floor beats the budget");
        // Floor of 3 reached; the budget is back in charge and long full.
        assert!(admission.try_acquire(1).is_none());
    }

    /// The ceiling beats the floor and the budget both.
    #[test]
    fn the_ceiling_caps_the_count_however_small_the_files() {
        let admission = budget(u64::MAX, 4, 8);
        let permits: Vec<_> = (0..8).map(|_| admission.try_acquire(1).unwrap()).collect();
        assert!(admission.try_acquire(1).is_none());
        drop(permits);
        assert!(admission.try_acquire(1).is_some());
    }

    /// A zero-byte file costs nothing, but still occupies a slot: it is a
    /// request either way.
    #[test]
    fn a_zero_byte_file_still_counts_against_the_ceiling() {
        let admission = budget(1_000, 1, 2);
        let _a = admission.try_acquire(0).unwrap();
        let _b = admission.try_acquire(0).unwrap();
        assert!(admission.try_acquire(0).is_none());
    }

    /// Dropping a permit gives its bytes back, or the budget would leak away to
    /// nothing over a long pull.
    #[test]
    fn dropping_a_permit_returns_its_bytes() {
        let admission = budget(1_000, 1, 16);
        let permit = admission.try_acquire(600).unwrap();
        assert!(admission.try_acquire(600).is_none());
        drop(permit);
        assert_eq!(admission.bytes_in_flight(), 0);
        assert_eq!(admission.in_flight(), 0);
        assert!(admission.try_acquire(600).is_some());
    }

    /// The waiting path: a blocked acquire resumes when room appears.
    #[tokio::test]
    async fn a_waiting_acquire_resumes_when_a_permit_is_released() {
        let admission = budget(1_000, 1, 16);
        let held = admission.try_acquire(1_000).unwrap();

        let waiter = tokio::spawn({
            let admission = admission.clone();
            async move { admission.acquire(800).await.cost() }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "must not admit over the budget");

        drop(held);
        assert_eq!(waiter.await.unwrap(), 800);
    }

    /// Misconfiguration must not wedge downloads entirely.
    #[test]
    fn a_nonsense_budget_is_sanitised_rather_than_deadlocking() {
        let admission = budget(0, 0, 0);
        assert_eq!(
            admission.budget(),
            Budget {
                bytes: 1,
                floor: 1,
                ceiling: 1
            }
        );
        assert!(admission.try_acquire(u64::MAX).is_some());
    }

    /// The default is the shipped policy; pin it so a change is deliberate.
    #[test]
    fn the_default_budget_is_the_documented_one() {
        let budget = Budget::default();
        assert_eq!(budget.bytes, 256 * 1024 * 1024);
        assert_eq!(budget.floor, 16);
        assert_eq!(budget.ceiling, 48);
    }
}
