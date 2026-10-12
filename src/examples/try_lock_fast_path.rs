//! Example 10 — a non-blocking `try_lock` fast path: probe, and bail out on
//! contention instead of queuing or spinning.
//!
//! `std::sync::Mutex::try_lock` exists too; the point here is the *posture*
//! of a lock that never sleeps: skip or defer work rather than block.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{Example, ExampleOutput};
use crate::SpinLock;

/// Probes with `try_lock()`; on `None` the work goes to a fallback counter
/// instead of waiting.
pub struct TryLockFastPathExample;

impl Example for TryLockFastPathExample {
    fn name(&self) -> &'static str {
        "Fast-path check-then-bail (try_lock)"
    }

    fn one_liner(&self) -> &'static str {
        "a non-blocking probe + fallback"
    }

    fn explanation(&self) -> &'static str {
        "try_lock() makes the non-blocking probe first-class: check whether \
         the lock is free, and if not, take the fallback path (skip or \
         defer work) instead of queuing. std::sync::Mutex::try_lock offers \
         the same shape, but the spinlock's benefit is the posture: a lock \
         that never sleeps fits a no-sleep context, where 'fall back' is the \
         only sane answer and the occasional skipped update is acceptable. \
         The run shows both paths: fast-path hits and fallback skips."
    }

    fn code(&self) -> &'static str {
        include_str!("try_lock_fast_path.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(TryLockFastPathExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const PROBES: usize = 100;
        const CONTENTION_MS: u64 = 10;

        // Working state, reset on every run.
        let data = Arc::new(SpinLock::new(0u64));
        let fast = Arc::new(AtomicU64::new(0));
        let fallback = Arc::new(AtomicU64::new(0));
        let cancel = out.cancel().clone();

        out.line("holder works under the lock in bursts; probers use try_lock()");
        out.line("a probe that misses takes the fallback path instead of blocking");

        // The holder, on its own thread, takes the lock in brief bursts.
        thread::scope(|s| {
            {
                let data = Arc::clone(&data);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    for _ in 0..3 {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut g = data.lock();
                        *g += 1;
                        thread::sleep(Duration::from_millis(CONTENTION_MS));
                        drop(g);
                        thread::sleep(Duration::from_millis(3));
                    }
                });
            }
            for _ in 0..4 {
                let data = Arc::clone(&data);
                let fast = Arc::clone(&fast);
                let fallback = Arc::clone(&fallback);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    for _ in 0..PROBES / 4 {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // The probe: no waiting, ever.
                        match data.try_lock() {
                            Some(mut g) => {
                                *g += 1;
                                fast.fetch_add(1, Ordering::Relaxed);
                            }
                            None => {
                                // Contended: skip rather than spin or park.
                                fallback.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        // A tiny pause so probes alternate with the bursts.
                        thread::sleep(Duration::from_micros(500));
                    }
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let f = fast.load(Ordering::Relaxed);
        let b = fallback.load(Ordering::Relaxed);
        out.line(format!("fast-path hits = {f}, fallback skips = {b}"));
        out.line(format!("total = {} probes resolved", f + b));

        true
    }
}
