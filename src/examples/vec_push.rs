//! Example 9 — the original `main()` demo: pushing into a `Vec` behind the
//! lock, relocated from the crate's first smoke test.
//!
//! Honest note: a `Vec::push` that may grow allocates on the heap inside the
//! critical section — exactly the case that favors a *sleeping* lock. We
//! keep it as the preserved demo, with the caveat said out loud.

use std::sync::Arc;
use std::thread;

use super::{Example, ExampleOutput};
use crate::SpinLock;

/// The original `main()` smoke test, as a re-runnable example.
pub struct VecPushExample;

impl Example for VecPushExample {
    fn name(&self) -> &'static str {
        "Vec push (original demo)"
    }

    fn one_liner(&self) -> &'static str {
        "the relocated smoke test"
    }

    fn explanation(&self) -> &'static str {
        "This is the crate's original main() demo, moved here rather than \
         deleted: two scoped threads push 1 and (2, 2) into a locked Vec. \
         Honest twist: Vec::push may grow, and a growth allocates on the \
         heap INSIDE the critical section — heap work is the classic \
         spinlock-killer, and a sleeping Mutex is the better tool there. \
         We keep the demo because it is the one everyone recognizes, with \
         the caveat stated out loud instead of hidden."
    }

    fn code(&self) -> &'static str {
        include_str!("vec_push.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(VecPushExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        // Working state, reset on every run.
        let x = Arc::new(SpinLock::new(Vec::new()));
        let cancel = out.cancel().clone();

        out.line("two scoped threads push 1 and (2, 2) behind the lock");

        thread::scope(|s| {
            s.spawn({
                let x = Arc::clone(&x);
                let cancel = Arc::clone(&cancel);
                move || {
                    if !cancel.load(std::sync::atomic::Ordering::Relaxed) {
                        x.lock().push(1);
                    }
                }
            });
            s.spawn({
                let x = Arc::clone(&x);
                let cancel = Arc::clone(&cancel);
                move || {
                    if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    let mut g = x.lock();
                    g.push(2);
                    g.push(2);
                }
            });
        });

        let cancelled = cancel.load(std::sync::atomic::Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let mut contents = x.lock().clone();
        contents.sort();
        out.line(format!(
            "note: a push that grows the Vec allocates in the section — a sleeping lock would fit better here"
        ));
        out.line(format!("contents (sorted) = {contents:?}"));

        true
    }
}
