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

use super::{Example, ExampleOutput, LaneState, VizSpec};
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
        const PRODUCER: usize = 0; // lane 0; consumers are lanes 1..=CONSUMERS

        // Working state, reset on every run.
        let queue = Arc::new(SpinLock::new(VecDeque::new()));
        let produced = Arc::new(AtomicU64::new(0));
        let consumed: Arc<[AtomicU64]> = (0..CONSUMERS).map(|_| AtomicU64::new(0)).collect();
        let done = Arc::new(AtomicBool::new(false));
        let cancel = out.cancel().clone();
        let mut lanes = vec![("producer".to_string(), TASKS as u64)];
        // How the consumers split the tasks is not known in advance (0 = no
        // per-lane progress bar); the total below is.
        lanes.extend((1..=CONSUMERS).map(|c| (format!("consumer {c}"), 0)));
        let viz = out.visualise(VizSpec {
            lock: "queue lock",
            data: "queue depth",
            capacity: None,
            unit: "tasks consumed",
            target: TASKS as u64,
            lanes,
        });

        out.line(format!(
            "{TASKS} tasks, one producer, {CONSUMERS} consumers on one locked VecDeque"
        ));

        thread::scope(|s| {
            // The producer runs concurrently with the consumers.
            {
                let queue = Arc::clone(&queue);
                let produced = Arc::clone(&produced);
                let done = Arc::clone(&done);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    for task in 0..TASKS {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut q = viz.lock(PRODUCER, &queue);
                        q.push_back(task as u64);
                        viz.level(q.len() as u64);
                        drop(q);
                        produced.fetch_add(1, Ordering::Relaxed);
                        viz.pause(1);
                    }
                    // Set only after the last push: see the consumer's check.
                    done.store(true, Ordering::Release);
                    viz.set_state(PRODUCER, LaneState::Done);
                });
            }
            for c in 0..CONSUMERS {
                let queue = Arc::clone(&queue);
                let consumed = Arc::clone(&consumed);
                let done = Arc::clone(&done);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    let lane = c + 1;
                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // Read `done` BEFORE popping: if the producer had
                        // already finished and the queue is empty now, it
                        // stays empty. (Checking afterwards could miss a
                        // task pushed between the pop and the check.)
                        let finished = done.load(Ordering::Acquire);
                        // Bind the result first: a guard created in the
                        // `match` scrutinee would stay held across the arms,
                        // and the `None` arm sleeps.
                        let popped = {
                            let mut q = viz.lock(lane, &queue);
                            let popped = q.pop_front();
                            viz.level(q.len() as u64);
                            popped
                        };
                        match popped {
                            Some(_task) => {
                                consumed[c].fetch_add(1, Ordering::Relaxed);
                                viz.fed(1);
                                // "Work" on the task, outside the lock.
                                viz.pause(6);
                            }
                            None if finished => break,
                            None => {
                                // Nothing yet; the producer is still running.
                                thread::sleep(Duration::from_micros(200));
                                viz.pause(1);
                            }
                        }
                    }
                    viz.set_state(lane, LaneState::Done);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let per_consumer: Vec<u64> = consumed.iter().map(|c| c.load(Ordering::Relaxed)).collect();
        let total: u64 = per_consumer.iter().sum();
        out.line(format!(
            "consumers popped while the producer was still pushing; split = {per_consumer:?}"
        ));
        out.line(format!(
            "produced = {}, consumed = {total}",
            produced.load(Ordering::Relaxed)
        ));

        true
    }
}
