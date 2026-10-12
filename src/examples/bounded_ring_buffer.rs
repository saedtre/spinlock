//! Example 8 — a fixed-capacity ring buffer: bounded and allocation-free.
//!
//! A ring that never grows, so the heap is never touched behind the lock —
//! and advancing two indices is a tiny section. For a real SPSC, a bounded
//! channel is often better; this sells the no-growth, no-alloc angle.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{Example, ExampleOutput};
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
                s.spawn(move || {
                    loop {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // Bind the result first: a guard created in the
                        // `match` scrutinee would stay held across the arms.
                        let popped = ring.lock().pop();
                        match popped {
                            Some(_v) => {
                                consumed.fetch_add(1, Ordering::Relaxed);
                            }
                            None => {
                                // Empty: sleep briefly instead of burning a core.
                                thread::sleep(Duration::from_micros(500));
                                // Use consistent atomic ordering
                                let produced_count = produced.load(Ordering::Relaxed);
                                if produced_count >= ITEMS {
                                    // Drain whatever the producer finished with.
                                    loop {
                                        let popped = ring.lock().pop();
                                        match popped {
                                            Some(_v) => {
                                                consumed.fetch_add(1, Ordering::Relaxed);
                                            }
                                            None => break,
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                });
            }
            // Producer: push into the ring, sleeping when it is full.
            {
                let ring = Arc::clone(&ring);
                let produced = Arc::clone(&produced);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    for i in 0..ITEMS {
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        loop {
                            if ring.lock().push(i) {
                                produced.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            // Full: sleep briefly; the ring never reallocates.
                            thread::sleep(Duration::from_micros(200));
                            if cancel.load(Ordering::Relaxed) {
                                break;
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
