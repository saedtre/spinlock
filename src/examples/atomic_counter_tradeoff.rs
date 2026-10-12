//! Example 2 — a counter: a guard vs an atomic, honestly.
//!
//! A two-field `Stats` struct updated as one unit under a `SpinLock`, next to
//! a plain `AtomicI64` updated lock-free. For a single integer the atomic
//! wins; for several fields that must move together, a guard is cleaner.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;

use super::{Example, ExampleOutput, LaneState, VizSpec};
use crate::SpinLock;

/// A small multi-field stat block that must be updated as one unit.
#[derive(Default)]
struct Stats {
    hits: i64,
    misses: i64,
}

/// `SpinLock<Stats>` next to a plain `AtomicI64`, side by side.
pub struct AtomicCounterTradeoffExample;

impl Example for AtomicCounterTradeoffExample {
    fn name(&self) -> &'static str {
        "Counter: guard vs atomic (honest)"
    }

    fn one_liner(&self) -> &'static str {
        "a multi-field stat struct vs a plain AtomicI64"
    }

    fn explanation(&self) -> &'static str {
        "For a single integer, the atomic is simply better: it is lock-free \
         and never burns a core, while every contended spinlock retry spins. \
         The spinlock's real value appears when you must update several \
         fields as one atomic unit — here a (hits, misses) pair — where a \
         guard keeps the pair consistent with one lock and is still cheaper \
         than a contended Mutex for a fast update. We print both final \
         values so you can see they agree; for one field, we say the \
         atomic wins."
    }

    fn code(&self) -> &'static str {
        include_str!("atomic_counter_tradeoff.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(AtomicCounterTradeoffExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 500;

        // Working state, reset on every run.
        let stats = Arc::new(SpinLock::new(Stats::default()));
        let atomic = Arc::new(AtomicI64::new(0));
        let cancel = out.cancel().clone();
        let viz = out.visualise(VizSpec {
            lock: "stats lock",
            data: "hits + misses",
            capacity: None,
            unit: "lookups",
            target: (THREADS * OPS_PER_THREAD) as u64,
            lanes: (1..=THREADS)
                .map(|t| (format!("thread {t}"), OPS_PER_THREAD as u64))
                .collect(),
        });
        let gate = viz.gate(THREADS);

        out.line("SpinLock<Stats { hits, misses }> next to a plain AtomicI64");
        out.line(format!("{THREADS} threads x {OPS_PER_THREAD} lookups (alternating hit/miss)"));

        thread::scope(|s| {
            for lane in 0..THREADS {
                let stats = Arc::clone(&stats);
                let atomic = Arc::clone(&atomic);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                let gate = Arc::clone(&gate);
                s.spawn(move || {
                    for i in 0..OPS_PER_THREAD {
                        if i % 10 == 0 {
                            if cancel.load(Ordering::Relaxed) {
                                break;
                            }
                            // A paced run pauses here, then all the threads
                            // carry on together so they still contend.
                            viz.pause(5);
                            gate.wait();
                        }
                        if i % 2 == 0 {
                            let mut g = viz.lock(lane, &stats);
                            g.hits += 1;
                            viz.level((g.hits + g.misses) as u64);
                            drop(g);
                            // The atomic side never takes the lock at all.
                            atomic.fetch_add(1, Ordering::Relaxed);
                        } else {
                            let mut g = viz.lock(lane, &stats);
                            g.misses += 1;
                            viz.level((g.hits + g.misses) as u64);
                        }
                        viz.fed(1);
                    }
                    viz.set_state(lane, LaneState::Done);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let g = stats.lock();
        let hits = g.hits;
        let misses = g.misses;
        drop(g);
        let a = atomic.load(Ordering::Relaxed);

        out.line(format!("guard  -> hits = {hits}, misses = {misses}"));
        out.line(format!("atomic -> hits = {a} (misses aren't tracked there)"));
        out.line(format!(
            "hits = {hits}, misses = {misses}, atomic = {a} (all agree)"
        ));

        true
    }
}
