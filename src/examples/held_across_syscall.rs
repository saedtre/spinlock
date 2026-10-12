//! Example 6 — holding the lock across a blocking operation: when NOT to use.
//!
//! The anti-example. While the holder sleeps (stand-in for a syscall or other
//! blocking op), the contenders can only spin: they burn cores for the whole
//! duration. A sleeping `Mutex` is the right tool there.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::viz::human;
use super::{Example, ExampleOutput, LaneState, VizSpec};
use crate::SpinLock;

/// The holder sleeps while holding the lock; two contenders spin behind it.
///
/// The run reports how long each contender spent busy-waiting.
pub struct HeldAcrossSyscallExample;

impl Example for HeldAcrossSyscallExample {
    fn name(&self) -> &'static str {
        "Held across a syscall (when NOT to use)"
    }

    fn one_liner(&self) -> &'static str {
        "a lock held across a blocking operation"
    }

    fn explanation(&self) -> &'static str {
        "This is the one to avoid. Holding any lock across a blocking \
         operation is bad, but with a spinlock it is actively harmful: every \
         contender can only busy-wait, so for the whole (multi-ms) hold they \
         burn 100% of a core each with nothing to do — a livelock risk on a \
         small machine. If your critical section must block (syscall, I/O, \
         wait), use a sleeping Mutex: contenders park and cost the CPU \
         nothing. Rule of thumb: a spinlock's section must never block. \
         (Separately: this lock does not poison — a panic in the section \
         releases the lock, but leaves the data as it was mid-update.)"
    }

    fn code(&self) -> &'static str {
        include_str!("held_across_syscall.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(HeldAcrossSyscallExample)
    }

    // snippet:start
    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const HOLDS: usize = 5;
        const SLEEP_MS: u64 = 20;
        const CONTENDERS: usize = 2;
        const CONTENTION_OPS: usize = 3;
        const HOLDER: usize = 0; // lane 0; contenders are lanes 1..=CONTENDERS

        // Working state, reset on every run.
        let value = Arc::new(SpinLock::new(0u32));
        let holding = Arc::new(AtomicBool::new(false));
        let held_ns = Arc::new(AtomicU64::new(0));
        let cancel = out.cancel().clone();
        let mut lanes = vec![("holder".to_string(), 1)];
        lanes.extend(
            (1..=CONTENDERS).map(|c| (format!("contender {c}"), 1 + CONTENTION_OPS as u64)),
        );
        let viz = out.visualise(VizSpec {
            lock: "value lock",
            data: "value",
            capacity: None,
            unit: "increments",
            target: (1 + CONTENDERS * (1 + CONTENTION_OPS)) as u64,
            lanes,
        });

        out.line(format!(
            "the holder will sleep {SLEEP_MS}ms {HOLDS} times WHILE holding the lock"
        ));
        out.line("two contenders each want the lock for one increment; they can only spin");
        if viz.paced() {
            out.line("(this paced run stretches each of those sleeps so the spinning can be watched)");
        }

        // Phase 1 — the bad habit: hold the lock across a blocking op (a
        // syscall stand-in). Each contender calls lock() once, while the
        // holder has it, and busy-waits for the whole hold.
        thread::scope(|s| {
            {
                let value = Arc::clone(&value);
                let holding = Arc::clone(&holding);
                let held_ns = Arc::clone(&held_ns);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    let mut g = viz.lock(HOLDER, &value);
                    let started = Instant::now();
                    holding.store(true, Ordering::Release);
                    for _ in 0..HOLDS {
                        // The blocking operation, inside the critical
                        // section. (A paced run stretches it so the spinning
                        // contenders can be watched.)
                        thread::sleep(Duration::from_millis(SLEEP_MS));
                        viz.pause(40);
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    *g += 1;
                    viz.level(u64::from(*g));
                    viz.fed(1);
                    held_ns.store(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    drop(g); // only now can the contenders get in
                    viz.set_state(HOLDER, LaneState::Done);
                });
            }
            for c in 0..CONTENDERS {
                let value = Arc::clone(&value);
                let holding = Arc::clone(&holding);
                let viz = viz.clone();
                s.spawn(move || {
                    // Wait until the holder really has the lock, so this is
                    // a contended acquisition every time.
                    while !holding.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                    // lock() spins here for as long as the holder sleeps:
                    // a whole core burned to do a single increment.
                    let mut g = viz.lock(c + 1, &value);
                    *g += 1;
                    viz.level(u64::from(*g));
                    viz.fed(1);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let held = Duration::from_nanos(held_ns.load(Ordering::Relaxed));
        let waits: Vec<Duration> = viz.snapshot().lanes[1..].iter().map(|l| l.spun).collect();
        for (c, wait) in waits.iter().enumerate() {
            out.line(format!(
                "contender {} busy-waited {} to do one increment",
                c + 1,
                human(*wait)
            ));
        }
        out.line(format!(
            "the holder held the lock for {}; the contenders burned {} of CPU spinning meanwhile",
            human(held),
            human(waits.iter().sum())
        ));

        // Phase 2 — after the release, each contender does a few trivial ops
        // under the now-free lock: the same work, with nothing to wait for.
        thread::scope(|s| {
            for c in 0..CONTENDERS {
                let value = Arc::clone(&value);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    for _ in 0..CONTENTION_OPS {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut g = viz.lock(c + 1, &value);
                        *g += 1;
                        viz.level(u64::from(*g));
                        drop(g);
                        viz.fed(1);
                        viz.pause(20);
                    }
                    viz.set_state(c + 1, LaneState::Done);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let total = *value.lock();
        out.line(format!(
            "holder released; final value = {total} (1 held + {CONTENDERS} contended + {} uncontended increments)",
            CONTENDERS * CONTENTION_OPS
        ));

        true
    }
}
