//! Example 4 — a shared task queue: one producer, several consumers.
//!
//! `mpsc` channels are point-to-point (a receiver can't be shared across
//! threads); a `VecDeque` under a lock is the simplest queue several
//! consumers can pop from, and a spinlock keeps the tiny pop/push off the
//! sleep path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{Example, ExampleOutput};
use crate::SpinLock;

/// One producer feeding one `SpinLock<VecDeque>` that several consumers drain.
pub struct TaskQueueExample;

impl Example for TaskQueueExample {
    fn name(&self) -> &'static str {
        "Shared task queue"
    }

    fn one_liner(&self) -> &'static str {
        "a VecDeque under the lock vs a channel"
    }

    fn explanation(&self) -> &'static str {
        "std::sync::mpsc is point-to-point: a receiver cannot be shared \
         across threads, so fanning one producer out to several consumers \
         through a channel is awkward. A VecDeque behind a lock is the \
         simplest queue N consumers can pop from, and the push/pop sections \
         are tiny, so a spinlock keeps them off the kernel sleep path. \
         Honest: for strict 1:1 with back-pressure, a std or crossbeam \
         channel is usually the better tool; this is the N-consumer case."
    }

    fn code(&self) -> &'static str {
        include_str!("task_queue.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(TaskQueueExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const TASKS: usize = 100;
        const CONSUMERS: usize = 3;

        // Working state, reset on every run.
        let queue = Arc::new(SpinLock::new(VecDeque::new()));
        let consumed = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let cancel = out.cancel().clone();

        out.line(format!(
            "{TASKS} tasks, one producer, {CONSUMERS} consumers on one locked VecDeque"
        ));

        // Produce first (inline), then let the consumers drain.
        let produced = {
            let mut produced = 0u64;
            for task in 0..TASKS {
                if cancel.load(Ordering::Relaxed) {
                    break;
                }
                queue.lock().push_back(task as u64);
                produced += 1;
            }
            done.store(true, Ordering::Release);
            produced
        };

        thread::scope(|s| {
            for _ in 0..CONSUMERS {
                let queue = Arc::clone(&queue);
                let consumed = Arc::clone(&consumed);
                let done = Arc::clone(&done);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        match queue.lock().pop_front() {
                            Some(_task) => {
                                consumed.fetch_add(1, Ordering::Relaxed);
                            }
                            None => {
                                if done.load(Ordering::Acquire) {
                                    break;
                                }
                                // Nothing yet; the producer is still running.
                                thread::sleep(Duration::from_micros(200));
                            }
                        }
                    }
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let total = consumed.load(Ordering::Relaxed);
        out.line("consumers popped concurrently under the same lock");
        out.line(format!("produced = {produced}, consumed = {total}"));

        true
    }
}
