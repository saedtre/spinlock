//! Example 5 — a no-sleep context: the canonical reason to spin.
//!
//! Models an interrupt / real-time / signal context that may not park or
//! sleep. There, a sleeping `Mutex` is unusable, so a busy-wait is the only
//! legal choice — it's legality, not speed.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::viz::human;
use super::{Example, ExampleOutput, LaneState, VizSpec};
use crate::SpinLock;

/// Two contexts that may not sleep share one spinlock; they poll a ready
/// flag with `spin_loop()` instead of parking.
pub struct NoSleepContextExample;

impl Example for NoSleepContextExample {
    fn name(&self) -> &'static str {
        "No-sleep context (the canonical reason)"
    }

    fn one_liner(&self) -> &'static str {
        "a context where blocking is forbidden"
    }

    fn explanation(&self) -> &'static str {
        "The canonical real-world rationale, and it is legality, not speed: \
         in a context that may not sleep — an interrupt handler, a real-time \
         task, a signal handler — a Mutex that parks is unusable, so a \
         busy-wait is the only legal wait. We accept the core burn because \
         the alternative (blocking) is forbidden in that context. Caveats: \
         worst on a single core, and the context must keep its sections \
         short; this is a compliance argument, not a performance one. Also \
         note this lock does not poison: if a section panics the lock is \
         released, but the data may be half-updated and nobody is told."
    }

    fn code(&self) -> &'static str {
        include_str!("no_sleep_context.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(NoSleepContextExample)
    }

    // snippet:start
    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const ITEMS: usize = 50;
        const CONTEXTS: usize = 2;
        /// How long a context polls for readiness before giving up.
        const POLL_LIMIT: Duration = Duration::from_secs(10);

        // Working state, reset on every run: 50 pending items, and a "ready"
        // flag the contexts poll instead of sleeping on. It starts false; a
        // separate "device" thread (which IS allowed to sleep) sets it a
        // moment after both contexts have started polling, so they really
        // do have to wait.
        let items = Arc::new(SpinLock::new(
            (0..ITEMS).map(|i| i as u64).collect::<Vec<u64>>(),
        ));
        let counter = Arc::new(SpinLock::new(0u64));
        let ready = Arc::new(AtomicBool::new(false));
        let processed = Arc::new(AtomicU64::new(0));
        let polls = Arc::new(AtomicU64::new(0));
        let polling = Arc::new(AtomicUsize::new(0));
        let timed_out = Arc::new(AtomicBool::new(false));
        let cancel = out.cancel().clone();
        let viz = out.visualise(VizSpec {
            lock: "counter lock",
            data: "counter",
            capacity: None,
            unit: "items",
            target: ITEMS as u64,
            // The contexts share the items; who gets how many is not known
            // in advance (0 = no per-lane progress bar).
            lanes: (1..=CONTEXTS).map(|c| (format!("context {c}"), 0)).collect(),
        });

        out.line("two no-sleep contexts (interrupt/RT style) share one spinlock");
        out.line("each 'await' is a bounded spin_loop() poll — parking is forbidden");

        thread::scope(|s| {
            // The device: ordinary code, so it may sleep before signalling.
            {
                let ready = Arc::clone(&ready);
                let polling = Arc::clone(&polling);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    // Become ready 2 ms after every context is waiting on us
                    // (a cancelled context never starts waiting).
                    while polling.load(Ordering::Acquire) < CONTEXTS
                        && !cancel.load(Ordering::Relaxed)
                    {
                        thread::yield_now();
                    }
                    thread::sleep(Duration::from_millis(2));
                    viz.pause(100);
                    ready.store(true, Ordering::Release);
                });
            }
            for lane in 0..CONTEXTS {
                let items = Arc::clone(&items);
                let counter = Arc::clone(&counter);
                let ready = Arc::clone(&ready);
                let processed = Arc::clone(&processed);
                let polls = Arc::clone(&polls);
                let polling = Arc::clone(&polling);
                let timed_out = Arc::clone(&timed_out);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // Bind the pop first, so the `items` lock is released
                        // before the poll below (a guard in a `while let`
                        // scrutinee would be held for the whole iteration).
                        let item = items.lock().pop();
                        let Some(item) = item else { break };

                        // The await phase: poll, because this context may
                        // not sleep. Bounded by a deadline and the cancel
                        // token.
                        if !ready.load(Ordering::Acquire) {
                            viz.set_state(lane, LaneState::Spinning);
                            polling.fetch_add(1, Ordering::Release);
                            let started = Instant::now();
                            let mut spins = 0u64;
                            while !ready.load(Ordering::Acquire) {
                                std::hint::spin_loop();
                                spins += 1;
                                if spins.is_multiple_of(1024)
                                    && (cancel.load(Ordering::Relaxed)
                                        || started.elapsed() > POLL_LIMIT)
                                {
                                    break;
                                }
                            }
                            polls.fetch_add(spins, Ordering::Relaxed);
                            viz.add_spin(lane, started.elapsed());
                            viz.set_state(lane, LaneState::Idle);
                            if !ready.load(Ordering::Acquire) {
                                if !cancel.load(Ordering::Relaxed) {
                                    timed_out.store(true, Ordering::Relaxed);
                                }
                                break;
                            }
                        }

                        // One unit of work; every 5th item chains one more.
                        let mut c = viz.lock(lane, &counter);
                        *c += 1 + u64::from(item % 5 == 0);
                        viz.level(*c);
                        drop(c);
                        processed.fetch_add(1, Ordering::Relaxed);
                        viz.fed(1);
                        // Display pause only (a no-op unless the TUI asked
                        // for a paced run). A real no-sleep context could
                        // not do this; it is here so the run can be watched.
                        viz.pause(10);
                    }
                    viz.set_state(lane, LaneState::Done);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }
        if timed_out.load(Ordering::Relaxed) {
            out.line(format!("device was not ready within {POLL_LIMIT:?}; gave up"));
            return true;
        }

        let total = *counter.lock();
        let spun: Duration = viz.snapshot().lanes.iter().map(|l| l.spun).sum();
        out.line(format!(
            "the contexts made {} spin_loop() polls ({} of busy-waiting) before the device was ready",
            polls.load(Ordering::Relaxed),
            human(spun)
        ));
        out.line(format!(
            "counter = {total} (each item +1, every 5th item +2)"
        ));
        out.line(format!(
            "no-sleep context processed {} items, counter = {total}",
            processed.load(Ordering::Relaxed)
        ));

        true
    }
}
