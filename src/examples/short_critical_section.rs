//! Example 1 — a few-ns critical section: the spinlock's sweet spot.
//!
//! Two threads hammer a shared counter whose critical section is a single
//! add. For sections this short, a tight `spin_loop` retry in user space is
//! cheaper than the futex round-trip a contended `Mutex` pays.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use super::{Example, ExampleOutput, LaneState, VizSpec};
use crate::SpinLock;

/// Few-ns work under the lock: increment a shared counter.
pub struct ShortCriticalSectionExample;

impl Example for ShortCriticalSectionExample {
    fn name(&self) -> &'static str {
        "Short critical section"
    }

    fn one_liner(&self) -> &'static str {
        "few-ns work under the lock"
    }

    fn explanation(&self) -> &'static str {
        "The critical section here is one atomic add — a few nanoseconds. A \
         contended std::sync::Mutex pays for a futex transition (a kernel \
         round-trip, µs scale) the first time it blocks, which dwarfs a \
         section this short; a tight spin_loop retry in user space is \
         cheaper while the section stays below that cost. Honest caveats: \
         the win shrinks as the section lengthens and disappears once it \
         exceeds a futex transition, and on a single core the contender \
         burns 100% of that core instead of yielding, so this is a \
         multicore story."
    }

    fn code(&self) -> &'static str {
        include_str!("short_critical_section.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(ShortCriticalSectionExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const THREADS: usize = 2;
        const BATCHES: usize = 10;
        const OPS_PER_BATCH: usize = 100;
        const OPS_PER_THREAD: usize = BATCHES * OPS_PER_BATCH;

        // Working state, reset on every run.
        let counter = Arc::new(SpinLock::new(0u64));
        let locked_ns = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(THREADS));
        let cancel = out.cancel().clone();
        let viz = out.visualise(VizSpec {
            lock: "counter lock",
            data: "counter",
            capacity: None,
            unit: "increments",
            target: (THREADS * OPS_PER_THREAD) as u64,
            lanes: (1..=THREADS)
                .map(|t| (format!("thread {t}"), OPS_PER_THREAD as u64))
                .collect(),
        });
        let gate = viz.gate(THREADS);

        out.line(format!(
            "{THREADS} threads x {OPS_PER_THREAD} increments behind the lock"
        ));

        thread::scope(|s| {
            for lane in 0..THREADS {
                let counter = Arc::clone(&counter);
                let locked_ns = Arc::clone(&locked_ns);
                let start = Arc::clone(&start);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                let gate = Arc::clone(&gate);
                s.spawn(move || {
                    // Start together, so the threads really do contend.
                    start.wait();
                    for _ in 0..BATCHES {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // Time only lock + increment + unlock: no thread
                        // spawn, no display pause, and no bookkeeping beyond
                        // a local count of how often the lock was busy.
                        let mut busy = 0u64;
                        let started = Instant::now();
                        for _ in 0..OPS_PER_BATCH {
                            let mut guard = match counter.try_lock() {
                                Some(guard) => guard,
                                None => {
                                    busy += 1;
                                    counter.lock() // spin until it is free
                                }
                            };
                            *guard += 1;
                        }
                        locked_ns.fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);

                        // Tell the picture about the batch afterwards.
                        viz.record(lane, OPS_PER_BATCH as u64);
                        viz.add_busy(lane, busy);
                        viz.fed(OPS_PER_BATCH as u64);
                        viz.level(*counter.lock());
                        // A paced run pauses here; the gate then starts both
                        // threads' next batch together, so they still meet
                        // at the lock.
                        viz.pause(25);
                        gate.wait();
                    }
                    viz.set_state(lane, LaneState::Done);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let total = *counter.lock();
        if total > 0 {
            let ns = locked_ns.load(Ordering::Relaxed);
            out.line(format!(
                "lock + increment + unlock took {:.1} ns/op on average ({total} ops, {ns} ns in the locked loops)",
                ns as f64 / total as f64
            ));
            out.line("(a debug build inflates this; `cargo run --release` shows the real cost)");
            let busy: u64 = viz.snapshot().lanes.iter().map(|l| l.busy).sum();
            out.line(format!(
                "the threads found the lock busy {busy} times and spun until it was free"
            ));
        }
        out.line(format!("final counter = {total}"));

        true
    }
}
