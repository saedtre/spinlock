//! Example 7 — sharding: N independent locks instead of one global lock.
//!
//! Hash a key to a shard and take only that shard's spinlock. Sharding cuts
//! contention to per-key; a *cheap* guard is a good fit because each shard's
//! section is short. Sharding helps with any guard — the spinlock is just
//! the light one.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use super::{Example, ExampleOutput};
use crate::SpinLock;

/// Eight independent `SpinLock<i64>` shards, addressed by key hash.
pub struct ShardedLocksExample;

impl Example for ShardedLocksExample {
    fn name(&self) -> &'static str {
        "Sharded / per-key locking"
    }

    fn one_liner(&self) -> &'static str {
        "N independent shards, each a SpinLock"
    }

    fn explanation(&self) -> &'static str {
        "Eight independent SpinLocks stand in for per-key locking: a \
         key hashes to its shard, and the section under that one shard's \
         guard is short. Sharding is the real win — it cuts contention from \
         global to per-key regardless of guard type — and a spinlock is a \
         good match because each shard's section is tiny. Honest caveat: \
         sharding also works fine with std Mutexes; the spinlock is simply \
         the lighter per-shard option."
    }

    fn code(&self) -> &'static str {
        include_str!("sharded_locks.rs")
    }

    fn clone_box(&self) -> Box<dyn Example> {
        Box::new(ShardedLocksExample)
    }

    fn run(&mut self, out: &mut ExampleOutput) -> bool {
        const SHARDS: usize = 8;
        const THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 25;

        // Working state, reset on every run: one fresh SpinLock per shard.
        let shards: Arc<[SpinLock<i64>]> =
            Arc::new(std::array::from_fn::<SpinLock<i64>, SHARDS, _>(|_| {
                SpinLock::new(0i64)
            }));
        // A global-lock baseline for contrast: the same keys, one lock.
        let global = Arc::new(SpinLock::new(0i64));
        let cancel = out.cancel().clone();

        out.line("keys hash to 8 shards; each shard takes only its own spinlock");
        out.line(format!(
            "{THREADS} threads x {OPS_PER_THREAD} key updates; the same keys also hit one global lock"
        ));

        // Each thread owns a disjoint key range, so every key is served
        // exactly once regardless of interleaving (deterministic totals).
        thread::scope(|s| {
            for t in 0..THREADS {
                let shards = Arc::clone(&shards);
                let global = Arc::clone(&global);
                let cancel = Arc::clone(&cancel);
                s.spawn(move || {
                    for i in 0..OPS_PER_THREAD {
                        let key = t * OPS_PER_THREAD + i;
                        if i % 5 == 0 && cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        // The shard path: only this key's shard is locked.
                        *shards[key % SHARDS].lock() += 1;
                        // The contrast path: the same keys under one lock.
                        *global.lock() += 1;
                    }
                });
            }
        });

        let cancelled = cancel.load(Ordering::Relaxed);
        if cancelled {
            return false;
        }

        let per_shard: Vec<i64> = (0..SHARDS).map(|i| *shards[i].lock()).collect();
        let total: i64 = per_shard.iter().sum();
        let g_total = *global.lock();

        out.line(format!("shards: {per_shard:?}"));
        out.line(format!(
            "one global lock served the same {g_total} updates, all serialized"
        ));
        out.line(format!(
            "total = {total} across {SHARDS} shards {per_shard:?}"
        ));

        true
    }
}
