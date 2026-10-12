//! Example 8 — a fixed-capacity ring buffer: bounded and allocation-free.
//!
//! A ring that never grows, so the heap is never touched behind the lock —
//! and advancing two indices is a tiny section. For a real SPSC, a bounded
//! channel is often better; this sells the no-growth, no-alloc angle.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{Example, ExampleOutput, LaneState, VizSpec};
use crate::SpinLock;

/// A fixed-capacity ring: pre-allocated slots, no heap growth, ever.
///
/// Full/empty is tracked with an explicit `filled` count: with plain
/// head/tail index arithmetic, a *full* ring (head == tail after the last
/// wrap) is indistinguishable from an empty one.
struct Ring {
    buf: Vec<Option<u64>>,
    head: usize,
    tail: usize,
    filled: usize,
}

impl Ring {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: vec![None; capacity],
            head: 0,
            tail: 0,
            filled: 0,
        }
    }

    fn len(&self) -> usize {
        self.filled
    }

    /// Returns `true` on success, `false` when full.
    fn push(&mut self, v: u64) -> bool {
        let n = self.buf.len();
        if self.filled == n {
            return false;
        }
        self.buf[self.tail] = Some(v);
        self.tail = (self.tail + 1) % n;
        self.filled += 1;
        true
    }

    /// Returns `None` when empty.
    fn pop(&mut self) -> Option<u64> {
        if self.filled == 0 {
            return None;
        }
        self.filled -= 1;
        let v = self.buf[self.head].take();
        self.head = (self.head + 1) % self.buf.len();
        v
    }
}

/// One producer and one consumer on a locked fixed-size ring.
pub struct BoundedRingBufferExample;

impl Example for BoundedRingBufferExample {
    fn name(&self) -> &'static str {
        "Fixed-size ring buffer"
    }

    fn one_liner(&self) -> &'static str {
        "a bounded ring, allocation-free"
    }

    fn explanation(&self) -> &'static str {
        "A fixed-capacity ring never grows, so there is no heap allocation \
         behind the lock — heap work in the section is the classic \
         spinlock-killer — and push/pop are two index updates: a tiny \
         section. Full/empty waits are a short sleep, which the example \
         calls out as the honest compromise (a single core or a tight SPSC \
         wants a lock-free ring or crossbeam's bounded channel instead)."
    }

    fn code(&self) -> &'static str {
        include_str!("bounded_ring_buffer.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(BoundedRingBufferExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const CAPACITY: usize = 8;
        const ITEMS: u64 = 100;

        // Working state, reset on every run.
        let ring = Arc::new(SpinLock::new(Ring::with_capacity(CAPACITY)));
        let produced = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let consumed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let cancel = out.cancel().clone();
        const PRODUCER: usize = 0;
        const CONSUMER: usize = 1;
        let viz = out.visualise(VizSpec {
            lock: "ring lock",
            data: "ring",
            capacity: Some(CAPACITY as u64),
            unit: "items consumed",
            target: ITEMS,
            // Expected acquisitions are a floor: a full or empty ring costs
            // extra visits to the lock.
            lanes: vec![("producer".to_string(), ITEMS), ("consumer".to_string(), ITEMS)],
        });

        out.line(format!(
            "fixed capacity {CAPACITY}: the ring never grows or allocates"
        ));
        out.line(format!(
            "one producer feeds {ITEMS} items; the consumer drains it"
        ));

        thread::scope(|s| {
            // Consumer: drains until the producer is done and the ring is empty.
            {
                let ring = Arc::clone(&ring);
                let consumed = Arc::clone(&consumed);
                let produced = Arc::clone(&produced);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    // One pop under the lock; the guard is gone on return.
                    let pop = || {
                        let mut r = viz.lock(CONSUMER, &ring);
                        let popped = r.pop();
                        viz.level(r.len() as u64);
                        popped
                    };
                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // Bind the result first: a guard created in the
                        // `match` scrutinee would stay held across the arms.
                        let popped = pop();
                        match popped {
                            Some(_v) => {
                                consumed.fetch_add(1, Ordering::Relaxed);
                                viz.fed(1);
                                viz.pause(3);
                            }
                            None => {
                                // Empty: sleep briefly instead of burning a core.
                                thread::sleep(Duration::from_micros(500));
                                viz.pause(1);
                                // Use consistent atomic ordering
                                let produced_count = produced.load(Ordering::Relaxed);
                                if produced_count >= ITEMS {
                                    // Drain whatever the producer finished with.
                                    loop {
                                        let popped = pop();
                                        match popped {
                                            Some(_v) => {
                                                consumed.fetch_add(1, Ordering::Relaxed);
                                                viz.fed(1);
                                                viz.pause(3);
                                            }
                                            None => break,
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    viz.set_state(CONSUMER, LaneState::Done);
                });
            }
            // Producer: push into the ring, sleeping when it is full.
            {
                let ring = Arc::clone(&ring);
                let produced = Arc::clone(&produced);
                let cancel = Arc::clone(&cancel);
                let viz = viz.clone();
                s.spawn(move || {
                    for i in 0..ITEMS {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        loop {
                            let pushed = {
                                let mut r = viz.lock(PRODUCER, &ring);
                                let pushed = r.push(i);
                                viz.level(r.len() as u64);
                                pushed
                            };
                            if pushed {
                                produced.fetch_add(1, Ordering::Relaxed);
                                viz.pause(2);
                                break;
                            }
                            // Full: sleep briefly; the ring never reallocates.
                            thread::sleep(Duration::from_micros(200));
                            viz.pause(1);
                            if cancel.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                    }
                    viz.set_state(PRODUCER, LaneState::Done);
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let dropped = ITEMS - consumed.load(Ordering::Relaxed);
        let remain = ring.lock().len();
        out.line(format!(
            "ring drained: {} items in flight at the end",
            remain
        ));
        out.line(format!(
            "produced = {}, consumed = {}, dropped = {}",
            produced.load(Ordering::Relaxed),
            consumed.load(Ordering::Relaxed),
            dropped
        ));

        true
    }
}
