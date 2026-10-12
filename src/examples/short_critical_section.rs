//! Example 1 — a few-ns critical section: the spinlock's sweet spot.
//!
//! Two threads hammer a shared counter whose critical section is a single
//! add. For sections this short, a tight `spin_loop` retry in user space is
//! cheaper than the futex round-trip a contended `Mutex` pays.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use super::{Example, ExampleOutput};
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
        const OPS_PER_THREAD: usize = 1_000;

        // Working state, reset on every run.
        let counter = Arc::new(SpinLock::new(0u64));
        let cancel = out.cancel().clone();

        out.line(format!(
            "{THREADS} threads x {OPS_PER_THREAD} increments behind the lock"
        ));

        let started = Instant::now();
        thread::scope(|s| {
            for _ in 0..THREADS {
                let counter = Arc::clone(&counter);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    for _ in 0..OPS_PER_THREAD {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        *counter.lock() += 1;
                    }
                });
            }
        });
        let elapsed = started.elapsed();

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let total = *counter.lock();
        if total > 0 {
            out.line(format!(
                "measured {total} increments in {} ns ({:.2} ns/op)",
                elapsed.as_nanos(),
                elapsed.as_nanos() as f64 / total as f64
            ));
        }
        out.line(format!("final counter = {total}"));

        true
    }
}
