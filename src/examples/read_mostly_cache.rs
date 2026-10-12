//! Example 3 — a read-mostly cache, honestly.
//!
//! A `HashMap` cache taking mostly-read traffic under one single-flag
//! `SpinLock`. The flag serializes readers too, so a read-heavy workload
//! usually favors `RwLock` or a sharded map — we say so.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use super::{Example, ExampleOutput};
use crate::SpinLock;

/// A `HashMap` cache whose reads and the rare inserts share one spinlock.
pub struct ReadMostlyCacheExample;

impl Example for ReadMostlyCacheExample {
    fn name(&self) -> &'static str {
        "Read-mostly cache (honest)"
    }

    fn one_liner(&self) -> &'static str {
        "a HashMap cache, mostly reads"
    }

    fn explanation(&self) -> &'static str {
        "Honest one: a single-flag lock serializes all access — readers \
         block on writers, and readers block on each other. A read-heavy \
         cache usually favors an RwLock (concurrent readers) or a sharded \
         map. A spinlock is only defensible here when each read section is \
         a few nanoseconds (a couple of hash probes) so the single flag's \
         cheap hop beats RwLock's reader bookkeeping — and even then, only \
         on multiple cores. For a typical read-mostly cache: no, reach for \
         RwLock."
    }

    fn code(&self) -> &'static str {
        include_str!("read_mostly_cache.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(ReadMostlyCacheExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 50;

        // Working state, reset on every run: an empty cache; workers 0 and 1
        // each insert one key; all four do 50 lookups each (mostly misses).
        let cache = Arc::new(SpinLock::new(HashMap::<String, u64>::new()));
        let lookups = Arc::new(AtomicU64::new(0));
        let inserts = Arc::new(AtomicU64::new(0));
        let cancel = out.cancel().clone();

        out.line("one SpinLock<HashMap<String, u64>> for all reads and writes");
        out.line(format!("{THREADS} threads x {OPS_PER_THREAD} lookups; 2 one-off inserts"));

        thread::scope(|s| {
            for worker in 0..THREADS {
                let cache = Arc::clone(&cache);
                let lookups = Arc::clone(&lookups);
                let inserts = Arc::clone(&inserts);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    for j in 0..OPS_PER_THREAD {
                        if j % 10 == 0 && cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut g = cache.lock();
                        let key = if j % 2 == 0 { "hot" } else { "cold" };
                        let _hit = g.get(key).copied();
                        if j == 0 && worker < 2 {
                            g.insert(format!("k{worker}"), worker as u64);
                            inserts.fetch_add(1, Ordering::Relaxed);
                        }
                        drop(g);
                        lookups.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let size = cache.lock().len();
        out.line(format!("cache ended with {size} entries (2 inserts, the rest were misses)"));
        out.line(format!(
            "size = {size}, lookups = {}, inserts = {}",
            lookups.load(Ordering::Relaxed),
            inserts.load(Ordering::Relaxed)
        ));

        true
    }
}
