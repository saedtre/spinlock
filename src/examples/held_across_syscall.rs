//! Example 6 — holding the lock across a blocking operation: when NOT to use.
//!
//! The anti-example. While the holder sleeps (stand-in for a syscall or other
//! blocking op), the contenders can only spin: they burn cores for the whole
//! duration. A sleeping `Mutex` is the right tool there.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{Example, ExampleOutput};
use crate::SpinLock;

/// The holder sleeps while holding the lock; two contenders spin behind it.
pub struct HeldAcrossSyscallExample;

impl Example for HeldAcrossSyscallExample {
    fn name(&self) -> &'static str {
        "Held across a syscall (when NOT to use)"
    }

    fn one_liner(&self) -> &'static str {
        "a lock held across a blocking operation"
    }

    fn explanation(&self) -> &'static str {
        "The required 'when NOT' exhibit. Holding any lock across a blocking \
         operation is bad, but with a spinlock it is actively harmful: every \
         contender can only busy-wait, so for the whole (multi-ms) hold they \
         burn 100% of a core each with nothing to do — a livelock risk on a \
         small machine. If your critical section must block (syscall, I/O, \
         wait), use a sleeping Mutex: contenders park and cost the CPU \
         nothing. Rule of thumb: a spinlock's section must never block."
    }

    fn code(&self) -> &'static str {
        include_str!("held_across_syscall.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(HeldAcrossSyscallExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const HOLDS: usize = 5;
        const SLEEP_MS: u64 = 20;
        const CONTENTION_OPS: usize = 3;

        // Working state, reset on every run.
        let flag = Arc::new(SpinLock::new(0u32));
        let released = Arc::new(AtomicBool::new(false));
        let cancel = out.cancel().clone();

        out.line(format!(
            "the holder will sleep {SLEEP_MS}ms {HOLDS} times WHILE holding the lock"
        ));
        out.line("two contenders spin behind it; each then does a few trivial ops");

        // Phase 1 — the bad habit: hold the lock across a blocking op (a
        // syscall stand-in). The contenders spin for the entire hold.
        let mut spins_seen = Vec::new();
        thread::scope(|s| {
            {
                let flag = Arc::clone(&flag);
                let released = Arc::clone(&released);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    let mut g = flag.lock();
                    *g = 1;
                    for _ in 0..HOLDS {
                        thread::sleep(Duration::from_millis(SLEEP_MS));
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    *g = 2;
                    released.store(true, Ordering::Release); // g drops here
                });
            }
            for _ in 0..2 {
                let flag = Arc::clone(&flag);
                let released = Arc::clone(&released);
                let cancel = Arc::clone(&cancel);
                let spins = Arc::new(AtomicU64::new(0));
                let spins_out = Arc::clone(&spins);
                s.spawn(move || {
                    // While the holder sleeps, lock() spins here: burning a
                    // core just to do a single increment.
                    while !released.load(Ordering::Acquire) && !cancel.load(Ordering::Relaxed) {
                        *flag.lock() += 1;
                        spins.fetch_add(1, Ordering::Relaxed);
                    }
                });
                spins_seen.push(Arc::clone(&spins_out));
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        // Phase 2 — after the release, each contender does a few trivial ops
        // under the now-free lock. These are the counted "serviced" ops.
        let mut serviced = Vec::new();
        thread::scope(|s| {
            for _ in 0..2 {
                let flag = Arc::clone(&flag);
                let cancel = Arc::clone(&cancel);
                let serviced_count = Arc::new(AtomicU64::new(0));
                let serviced_out = Arc::clone(&serviced_count);
                s.spawn(move || {
                    for _ in 0..CONTENTION_OPS {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        *flag.lock() += 1;
                        serviced_count.fetch_add(1, Ordering::Relaxed);
                    }
                });
                serviced.push(serviced_out);
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let spun = spins_seen
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum::<u64>();
        let done = serviced
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .min()
            .unwrap_or(0);
        out.line(format!(
            "while the holder slept ~{}ms, the contenders spun {spun} times burning cores",
            SLEEP_MS * HOLDS as u64
        ));
        out.line(format!("holder released; contender serviced {done} times"));

        true
    }
}
