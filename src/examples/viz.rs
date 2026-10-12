//! A small, shared picture of an example's traffic through its spinlock.
//!
//! An example describes itself once with a [`VizSpec`] (which threads take
//! part, what the lock guards, how much work is expected) and then reports
//! what it does through a [`Viz`] handle: [`Viz::lock`] / [`Viz::try_lock`]
//! wrap the real lock calls and record who is holding, who is busy-waiting
//! and for how long. The TUI holds a clone of the same handle and samples it
//! with [`Viz::snapshot`] on every redraw, so the picture is live.
//!
//! Two honest notes about what the picture shows:
//!
//! - It is **sampled**. A critical section of a few nanoseconds is almost
//!   never caught in the `HOLD` state; the counters (acquisitions, how often
//!   the lock was found busy, time spent spinning) are exact.
//! - The TUI asks examples to **pace** themselves ([`Viz::pause`]) so a run
//!   lasts long enough to watch. Pauses are placed outside critical sections
//!   unless an example says otherwise, and are no-ops in the tests. Threads
//!   that sleep separately wake at slightly different times and would then
//!   hardly ever meet at the lock, so the short-section examples line their
//!   threads up again after each pause with a [`Gate`].
//!
//! The bookkeeping here uses atomics plus one `std::sync::Mutex` for the
//! labels (strings allocate, so a sleeping lock is the right guard for them).

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use crate::{Guard, SpinLock};

/// The most threads ("lanes") one picture can show.
pub const MAX_LANES: usize = 8;

/// What a lane's thread is doing right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneState {
    /// Not in or waiting for a critical section.
    Idle,
    /// Busy-waiting (on the lock, or on a flag it may not sleep on).
    Spinning,
    /// Inside the critical section.
    Holding,
    /// Finished its work.
    Done,
}

impl LaneState {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => LaneState::Spinning,
            2 => LaneState::Holding,
            3 => LaneState::Done,
            _ => LaneState::Idle,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            LaneState::Idle => 0,
            LaneState::Spinning => 1,
            LaneState::Holding => 2,
            LaneState::Done => 3,
        }
    }

    fn label(self) -> &'static str {
        match self {
            LaneState::Idle => "idle",
            LaneState::Spinning => "SPIN",
            LaneState::Holding => "HOLD",
            LaneState::Done => "done",
        }
    }
}

/// How an example describes itself to the picture.
pub struct VizSpec {
    /// Name of the lock, e.g. `"queue lock"`.
    pub lock: &'static str,
    /// Name of the guarded data, e.g. `"queue depth"`.
    pub data: &'static str,
    /// Capacity of the guarded data, if it is bounded (draws a gauge).
    pub capacity: Option<u64>,
    /// What one unit of work is called, e.g. `"tasks"`.
    pub unit: &'static str,
    /// How many units are expected to pass through the lock in a full run.
    pub target: u64,
    /// One `(label, expected acquisitions)` per participating thread. Use
    /// `0` when a thread's share of the work is not known in advance (e.g.
    /// consumers racing for one queue): its lane is then drawn without a
    /// progress bar instead of one that could never fill.
    pub lanes: Vec<(String, u64)>,
}

#[derive(Default)]
struct Meta {
    lock: &'static str,
    data: &'static str,
    capacity: Option<u64>,
    unit: &'static str,
    target: u64,
    lanes: Vec<(String, u64)>,
}

#[derive(Default)]
struct Lane {
    state: AtomicU8,
    acquired: AtomicU64,
    busy: AtomicU64,
    spun_ns: AtomicU64,
}

struct Shared {
    meta: Mutex<Meta>,
    lanes: [Lane; MAX_LANES],
    fed: AtomicU64,
    level: AtomicU64,
    pace: Duration,
    cancel: Arc<AtomicBool>,
}

/// A meeting point for the threads of a paced run: [`Gate::wait`] returns in
/// every thread at (nearly) the same instant, once all of them have arrived.
///
/// Made by [`Viz::gate`]. In an unpaced run `wait` returns at once — threads
/// that never pause already overlap. Waiting is a busy-wait, not a sleep, so
/// that the threads are released within nanoseconds of each other; it ends
/// early if the run is cancelled. Every thread must call `wait` the same
/// number of times.
pub struct Gate {
    parties: usize,
    /// Threads that have arrived for the current round.
    arrived: Mutex<usize>,
    /// Bumped by the last thread to arrive; that releases the others.
    round: AtomicU64,
    paced: bool,
    cancel: Arc<AtomicBool>,
}

impl Gate {
    /// Wait until every party has called `wait` for this round.
    pub fn wait(&self) {
        if !self.paced {
            return;
        }
        let round = {
            let mut arrived = self.arrived.lock().unwrap_or_else(|e| e.into_inner());
            *arrived += 1;
            if *arrived == self.parties {
                *arrived = 0;
                self.round.fetch_add(1, Ordering::Release);
                return;
            }
            self.round.load(Ordering::Acquire)
        };
        while self.round.load(Ordering::Acquire) == round
            && !self.cancel.load(Ordering::Relaxed)
        {
            std::hint::spin_loop();
        }
    }
}

/// Handle to the shared picture. Cheap to clone; every clone sees the same
/// state, so an example's worker threads and the TUI can all hold one.
#[derive(Clone)]
pub struct Viz {
    shared: Arc<Shared>,
}

impl Viz {
    /// A picture with no lanes yet. `pace` is the length of one display
    /// pause unit (`Duration::ZERO` = never pause); `cancel` cuts pauses
    /// short.
    pub(super) fn new(cancel: Arc<AtomicBool>, pace: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                meta: Mutex::new(Meta::default()),
                lanes: Default::default(),
                fed: AtomicU64::new(0),
                level: AtomicU64::new(0),
                pace,
                cancel,
            }),
        }
    }

    fn meta(&self) -> MutexGuard<'_, Meta> {
        // The labels are plain data; a panic elsewhere cannot corrupt them.
        self.shared.meta.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reset every counter and install a new description.
    pub(super) fn configure(&self, spec: VizSpec) {
        assert!(
            spec.lanes.len() <= MAX_LANES,
            "a picture shows at most {MAX_LANES} lanes"
        );
        for lane in &self.shared.lanes {
            lane.state.store(LaneState::Idle.as_u8(), Ordering::Relaxed);
            lane.acquired.store(0, Ordering::Relaxed);
            lane.busy.store(0, Ordering::Relaxed);
            lane.spun_ns.store(0, Ordering::Relaxed);
        }
        self.shared.fed.store(0, Ordering::Relaxed);
        self.shared.level.store(0, Ordering::Relaxed);
        *self.meta() = Meta {
            lock: spec.lock,
            data: spec.data,
            capacity: spec.capacity,
            unit: spec.unit,
            target: spec.target,
            lanes: spec.lanes,
        };
    }

    /// `lock.lock()`, recorded for the picture: if the lock is busy the lane
    /// shows `SPIN` and the time spent busy-waiting is added to its total;
    /// once acquired it shows `HOLD` until the returned guard is dropped.
    pub fn lock<'a, T>(&'a self, lane: usize, lock: &'a SpinLock<T>) -> Traced<'a, T> {
        let guard = match lock.try_lock() {
            Some(guard) => guard,
            None => {
                let l = &self.shared.lanes[lane];
                l.busy.fetch_add(1, Ordering::Relaxed);
                l.state.store(LaneState::Spinning.as_u8(), Ordering::Relaxed);
                let started = Instant::now();
                let guard = lock.lock();
                l.spun_ns
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                guard
            }
        };
        self.acquired(lane, guard)
    }

    /// `lock.try_lock()`, recorded for the picture: a miss counts as one
    /// "found it busy" for the lane and returns `None` without waiting.
    pub fn try_lock<'a, T>(&'a self, lane: usize, lock: &'a SpinLock<T>) -> Option<Traced<'a, T>> {
        match lock.try_lock() {
            Some(guard) => Some(self.acquired(lane, guard)),
            None => {
                self.shared.lanes[lane].busy.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn acquired<'a, T>(&'a self, lane: usize, guard: Guard<'a, T>) -> Traced<'a, T> {
        let l = &self.shared.lanes[lane];
        l.acquired.fetch_add(1, Ordering::Relaxed);
        l.state.store(LaneState::Holding.as_u8(), Ordering::Relaxed);
        Traced {
            guard,
            viz: self,
            lane,
        }
    }

    /// Record `n` acquisitions made with the plain lock API (used where the
    /// per-call bookkeeping of [`Self::lock`] would distort a measurement).
    pub fn record(&self, lane: usize, n: u64) {
        self.shared.lanes[lane].acquired.fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` times a lane found the lock busy, counted by the caller
    /// (the companion of [`Self::record`]).
    pub fn add_busy(&self, lane: usize, n: u64) {
        self.shared.lanes[lane].busy.fetch_add(n, Ordering::Relaxed);
    }

    /// Set a lane's state directly (e.g. `Spinning` while polling a flag).
    pub fn set_state(&self, lane: usize, state: LaneState) {
        self.shared.lanes[lane]
            .state
            .store(state.as_u8(), Ordering::Relaxed);
    }

    /// Add time a lane spent busy-waiting on something other than the lock.
    pub fn add_spin(&self, lane: usize, spun: Duration) {
        self.shared.lanes[lane]
            .spun_ns
            .fetch_add(spun.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Count `n` units of work as having passed through the lock.
    pub fn fed(&self, n: u64) {
        self.shared.fed.fetch_add(n, Ordering::Relaxed);
    }

    /// Publish the current size / value of the guarded data. One atomic
    /// store, so it is cheap enough to call inside the critical section.
    pub fn level(&self, value: u64) {
        self.shared.level.store(value, Ordering::Relaxed);
    }

    /// True when the caller asked for a paced (watchable) run.
    pub fn paced(&self) -> bool {
        !self.shared.pace.is_zero()
    }

    /// Sleep for `units` display pause units so a person can follow the
    /// run. Returns at once when pacing is off (the tests) and stops early
    /// when the run is cancelled.
    pub fn pause(&self, units: u32) {
        if !self.paced() {
            return;
        }
        for _ in 0..units {
            if self.shared.cancel.load(Ordering::Relaxed) {
                return;
            }
            thread::sleep(self.shared.pace);
        }
    }

    /// A [`Gate`] for `parties` threads of this run: after a display pause,
    /// each calls [`Gate::wait`] so they all carry on together and still
    /// meet at the lock. A no-op in an unpaced run.
    pub fn gate(&self, parties: usize) -> Arc<Gate> {
        Arc::new(Gate {
            parties,
            arrived: Mutex::new(0),
            round: AtomicU64::new(0),
            paced: self.paced(),
            cancel: Arc::clone(&self.shared.cancel),
        })
    }

    /// A consistent-enough copy of the picture for drawing or asserting on.
    pub fn snapshot(&self) -> VizSnapshot {
        let meta = self.meta();
        let lanes = meta
            .lanes
            .iter()
            .zip(&self.shared.lanes)
            .map(|((label, expected), lane)| LaneSnapshot {
                label: label.clone(),
                expected: *expected,
                state: LaneState::from_u8(lane.state.load(Ordering::Relaxed)),
                acquired: lane.acquired.load(Ordering::Relaxed),
                busy: lane.busy.load(Ordering::Relaxed),
                spun: Duration::from_nanos(lane.spun_ns.load(Ordering::Relaxed)),
            })
            .collect();
        VizSnapshot {
            lock: meta.lock,
            data: meta.data,
            capacity: meta.capacity,
            unit: meta.unit,
            target: meta.target,
            fed: self.shared.fed.load(Ordering::Relaxed),
            level: self.shared.level.load(Ordering::Relaxed),
            lanes,
        }
    }
}

/// A [`Guard`] obtained through [`Viz::lock`] / [`Viz::try_lock`]. Behaves
/// like the guard it wraps; dropping it marks the lane idle and unlocks.
pub struct Traced<'a, T> {
    guard: Guard<'a, T>,
    viz: &'a Viz,
    lane: usize,
}

impl<T> Deref for Traced<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for Traced<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T> Drop for Traced<'_, T> {
    fn drop(&mut self) {
        // Runs before the wrapped guard is dropped (which is the unlock).
        self.viz.set_state(self.lane, LaneState::Idle);
    }
}

/// One lane of a [`VizSnapshot`].
#[derive(Clone, Debug)]
pub struct LaneSnapshot {
    pub label: String,
    /// Acquisitions this lane is expected to make in a full run; `0` if its
    /// share is not known in advance (the lane is drawn without a bar).
    pub expected: u64,
    pub state: LaneState,
    /// Times the lane acquired the lock.
    pub acquired: u64,
    /// Times the lane found the lock busy (then spun, or bailed out).
    pub busy: u64,
    /// Total time the lane spent busy-waiting.
    pub spun: Duration,
}

/// A copy of the picture at one instant.
#[derive(Clone, Debug)]
pub struct VizSnapshot {
    pub lock: &'static str,
    pub data: &'static str,
    pub capacity: Option<u64>,
    pub unit: &'static str,
    pub target: u64,
    /// Units of work that have passed through the lock so far.
    pub fed: u64,
    /// Current size / value of the guarded data.
    pub level: u64,
    pub lanes: Vec<LaneSnapshot>,
}

impl VizSnapshot {
    /// True until an example has described itself.
    pub fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }

    /// Draw the picture as plain text, at most `width` columns wide: one
    /// line for the lock and its data, one per lane, one for the total.
    pub fn render(&self, width: usize) -> Vec<String> {
        if self.is_empty() {
            return Vec::new();
        }
        let bar_width = width.saturating_sub(56).clamp(6, 24);
        let mut lines = Vec::with_capacity(self.lanes.len() + 2);

        let holders: Vec<&str> = self
            .lanes
            .iter()
            .filter(|l| l.state == LaneState::Holding)
            .map(|l| l.label.as_str())
            .collect();
        let held = if holders.is_empty() {
            "free".to_string()
        } else {
            format!("HELD by {}", holders.join(", "))
        };
        let data = match self.capacity {
            Some(capacity) => format!(
                "{} {} {}/{}",
                self.data,
                bar(self.level, capacity, capacity.min(16) as usize),
                self.level,
                capacity
            ),
            None => format!("{} = {}", self.data, self.level),
        };
        lines.push(format!("{}: {held}   ->   {data}", self.lock));

        for lane in &self.lanes {
            // No bar for a lane whose share is unknown: a bar measured
            // against the whole workload would end part-filled beside
            // `done`. The blank keeps the columns aligned.
            let progress = if lane.expected == 0 {
                " ".repeat(bar_width + 2)
            } else {
                bar(lane.acquired, lane.expected, bar_width)
            };
            let mut line = format!(
                "{:<12.12} {:<4} {} {:>5} acq",
                lane.label,
                lane.state.label(),
                progress,
                lane.acquired
            );
            if lane.busy > 0 {
                line.push_str(&format!("  {} busy", lane.busy));
            }
            if !lane.spun.is_zero() {
                line.push_str(&format!("  spun {}", human(lane.spun)));
            }
            lines.push(line);
        }

        lines.push(format!(
            "{:<17} {} {}/{} {}",
            "through the lock",
            bar(self.fed, self.target, bar_width),
            self.fed,
            self.target,
            self.unit
        ));

        lines
            .into_iter()
            .map(|l| l.chars().take(width).collect())
            .collect()
    }
}

/// A `width`-cell gauge showing `value` out of `max`.
fn bar(value: u64, max: u64, width: usize) -> String {
    let filled = if max == 0 {
        0
    } else {
        ((value.min(max) as u128 * width as u128) / max as u128) as usize
    };
    let mut s = String::with_capacity(width * 3 + 2);
    s.push('|');
    s.extend(std::iter::repeat_n('█', filled));
    s.extend(std::iter::repeat_n('░', width - filled));
    s.push('|');
    s
}

/// A duration as a short human-readable string.
pub fn human(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}us", ns as f64 / 1e3)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1e6)
    } else {
        format!("{:.2}s", ns as f64 / 1e9)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> VizSpec {
        VizSpec {
            lock: "test lock",
            data: "value",
            capacity: Some(8),
            unit: "items",
            target: 10,
            lanes: vec![("writer".to_string(), 10), ("a very long lane label".to_string(), 10)],
        }
    }

    fn viz() -> Viz {
        let viz = Viz::new(Arc::new(AtomicBool::new(false)), Duration::ZERO);
        viz.configure(spec());
        viz
    }

    #[test]
    fn lock_records_holding_then_idle() {
        let viz = viz();
        let lock = SpinLock::new(0u32);
        {
            let mut g = viz.lock(0, &lock);
            *g += 1;
            let snap = viz.snapshot();
            assert_eq!(snap.lanes[0].state, LaneState::Holding);
            assert_eq!(snap.lanes[0].acquired, 1);
            assert!(snap.render(80)[0].contains("HELD by writer"));
        }
        let snap = viz.snapshot();
        assert_eq!(snap.lanes[0].state, LaneState::Idle);
        assert!(snap.render(80)[0].contains("free"));
        // The traced guard really unlocked.
        assert_eq!(*lock.try_lock().expect("unlocked"), 1);
    }

    #[test]
    fn try_lock_miss_counts_busy_and_does_not_wait() {
        let viz = viz();
        let lock = SpinLock::new(0u32);
        let held = viz.lock(0, &lock);
        assert!(viz.try_lock(1, &lock).is_none());
        drop(held);
        assert!(viz.try_lock(1, &lock).is_some());
        let snap = viz.snapshot();
        assert_eq!(snap.lanes[1].busy, 1);
        assert_eq!(snap.lanes[1].acquired, 1);
    }

    #[test]
    fn contended_lock_records_spin_time() {
        let viz = viz();
        let lock = SpinLock::new(0u32);
        let holding = AtomicBool::new(false);
        thread::scope(|s| {
            s.spawn(|| {
                let _g = viz.lock(0, &lock);
                holding.store(true, Ordering::Release);
                thread::sleep(Duration::from_millis(20));
            });
            s.spawn(|| {
                while !holding.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                *viz.lock(1, &lock) += 1;
            });
        });
        let snap = viz.snapshot();
        assert_eq!(snap.lanes[1].busy, 1);
        assert!(
            snap.lanes[1].spun >= Duration::from_millis(10),
            "spun only {:?}",
            snap.lanes[1].spun
        );
    }

    #[test]
    fn configure_resets_counters() {
        let viz = viz();
        let lock = SpinLock::new(0u32);
        drop(viz.lock(0, &lock));
        viz.fed(3);
        viz.level(5);
        viz.configure(spec());
        let snap = viz.snapshot();
        assert_eq!((snap.fed, snap.level, snap.lanes[0].acquired), (0, 0, 0));
    }

    #[test]
    fn render_fits_any_width_and_has_one_line_per_lane() {
        let viz = viz();
        viz.fed(4);
        viz.level(3);
        viz.record(0, 4);
        viz.add_spin(1, Duration::from_millis(1234));
        let snap = viz.snapshot();
        for width in 1..=200 {
            let lines = snap.render(width);
            assert_eq!(lines.len(), snap.lanes.len() + 2);
            for line in &lines {
                assert!(line.chars().count() <= width, "{width}: {line:?}");
            }
        }
        let lines = snap.render(100);
        assert!(lines[0].contains("value") && lines[0].contains("3/8"));
        assert!(lines[1].starts_with("writer") && lines[1].contains("4 acq"));
        assert!(lines[2].contains("spun 1.23s"));
        assert!(lines[3].contains("4/10 items"));
    }

    #[test]
    fn a_lane_with_no_expected_count_is_drawn_without_a_bar() {
        let viz = Viz::new(Arc::new(AtomicBool::new(false)), Duration::ZERO);
        viz.configure(VizSpec {
            lanes: vec![("producer".to_string(), 10), ("consumer".to_string(), 0)],
            ..spec()
        });
        viz.record(0, 10);
        viz.record(1, 4);
        viz.add_busy(1, 2);
        let snap = viz.snapshot();
        for width in 1..=200 {
            for line in snap.render(width) {
                assert!(line.chars().count() <= width, "{width}: {line:?}");
            }
        }
        let lines = snap.render(100);
        assert!(lines[1].contains('█'), "{:?}", lines[1]);
        assert!(!lines[2].contains(['█', '░', '|']), "{:?}", lines[2]);
        assert!(lines[2].contains("4 acq  2 busy"), "{:?}", lines[2]);
        // Same columns with or without the bar.
        let column = |l: &str| l.split(" acq").next().unwrap().chars().count();
        assert_eq!(column(&lines[1]), column(&lines[2]));
    }

    /// A paced gate releases every thread only once all have arrived, and
    /// lets a cancelled run go.
    #[test]
    fn gate_releases_all_parties_together_and_on_cancel() {
        const PARTIES: usize = 3;
        const ROUNDS: usize = 20;
        let cancel = Arc::new(AtomicBool::new(false));
        let paced = Viz::new(Arc::clone(&cancel), Duration::from_micros(1));
        let gate = paced.gate(PARTIES);
        let waiting = AtomicU64::new(0);
        thread::scope(|s| {
            for t in 0..PARTIES {
                let (gate, waiting) = (&gate, &waiting);
                s.spawn(move || {
                    for round in 0..ROUNDS {
                        // Stagger the arrivals.
                        thread::sleep(Duration::from_micros(200 * t as u64));
                        waiting.fetch_add(1, Ordering::SeqCst);
                        gate.wait();
                        // Nobody gets through before everyone has arrived.
                        let arrived = waiting.load(Ordering::SeqCst) as usize;
                        assert!(arrived >= PARTIES * (round + 1), "{arrived} in round {round}");
                    }
                });
            }
        });

        // One party alone would wait for ever; cancelling lets it go.
        let alone = paced.gate(2);
        thread::scope(|s| {
            s.spawn(|| alone.wait());
            thread::sleep(Duration::from_millis(5));
            cancel.store(true, Ordering::Relaxed);
        });

        // Unpaced: never waits at all, even alone.
        let unpaced = viz().gate(2);
        unpaced.wait();
        unpaced.wait();
    }

    #[test]
    fn empty_picture_renders_nothing() {
        let viz = Viz::new(Arc::new(AtomicBool::new(false)), Duration::ZERO);
        assert!(viz.snapshot().is_empty());
        assert!(viz.snapshot().render(80).is_empty());
    }

    #[test]
    fn pause_is_a_no_op_when_unpaced_and_stops_on_cancel() {
        let unpaced = viz();
        let started = Instant::now();
        unpaced.pause(1_000_000);
        assert!(started.elapsed() < Duration::from_secs(1));

        let cancel = Arc::new(AtomicBool::new(true));
        let paced = Viz::new(Arc::clone(&cancel), Duration::from_millis(50));
        let started = Instant::now();
        paced.pause(1_000);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
