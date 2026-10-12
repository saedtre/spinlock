//! Example 5 — a no-sleep context: the canonical reason to spin.
//!
//! Models an interrupt / real-time / signal context that may not park or
//! sleep. There, a sleeping `Mutex` is unusable, so a busy-wait is the only
//! legal choice — it's legality, not speed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use super::{Example, ExampleOutput};
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
         short; this is a compliance argument, not a performance one."
    }

    fn code(&self) -> &'static str {
        include_str!("no_sleep_context.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(NoSleepContextExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const ITEMS: usize = 50;
        const CONTEXTS: usize = 2;

        // Working state, reset on every run: 50 pending items, and a "ready"
        // flag the contexts will poll instead of sleeping on. It is set up
        // front so the bounded poll below succeeds on its first try.
        let items = Arc::new(SpinLock::new(
            (0..ITEMS).map(|i| i as u64).collect::<Vec<u64>>(),
        ));
        let counter = Arc::new(SpinLock::new(0u64));
        let ready = Arc::new(AtomicBool::new(true));
        let processed = Arc::new(AtomicU64::new(0));
        let cancel = out.cancel().clone();

        out.line("two no-sleep contexts (interrupt/RT style) share one spinlock");
        out.line("each 'await' is a bounded spin_loop() poll — parking is forbidden");

        thread::scope(|s| {
            for _ in 0..CONTEXTS {
                let items = Arc::clone(&items);
                let counter = Arc::clone(&counter);
                let ready = Arc::clone(&ready);
                let processed = Arc::clone(&processed);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    while let Some(item) = items.lock().pop() {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // The await phase: poll, because this context may not sleep.
                        for _ in 0..1_000 {
                            if ready.load(Ordering::Acquire) {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                        // One unit of work; every 5th item chains one more.
                        let mut c = counter.lock();
                        *c += 1 + u64::from(item % 5 == 0);
                        processed.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let total = *counter.lock();
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
