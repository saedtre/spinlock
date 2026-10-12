//! The ten spinlock-consumer examples, shared by the TUI and the test harness.
//!
//! Each example is a self-contained [`Example`] object. The design rule every
//! example follows:
//!
//! - `run` **resets its own working state at the top**, so a `clone_box()` is
//!   re-runnable and idempotent;
//! - it uses **bounded iteration counts** (so a run always terminates, even if
//!   the cancel token is ignored);
//! - it **polls [`ExampleOutput::is_cancelled`]** periodically and returns
//!   `false` from `run` if it stops early because of cancellation;
//! - it writes **every visible line** via [`ExampleOutput::line`];
//! - it **always emits a deterministic final summary line** that the tests
//!   assert on (variable / interleaved lines are informational only).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub mod atomic_counter_tradeoff;
pub mod bounded_ring_buffer;
pub mod held_across_syscall;
pub mod no_sleep_context;
pub mod read_mostly_cache;
pub mod sharded_locks;
pub mod short_critical_section;
pub mod task_queue;
pub mod try_lock_fast_path;
pub mod vec_push;

/// Output sink for a running example: captured visible lines plus a
/// cooperative cancel token.
///
/// `lines` accumulates whatever the example writes via [`Self::line`]; the
/// caller (the TUI, or a test) drains it afterwards. `cancel` is shared with
/// the example via an [`Arc`] so the surrounding event loop can ask a running
/// example to stop.
pub struct ExampleOutput {
    lines: Vec<String>,
    cancel: Arc<AtomicBool>,
}

impl ExampleOutput {
    /// Create a fresh output sink that shares the given cancel token.
    pub fn new(cancel: Arc<AtomicBool>) -> Self {
        Self {
            lines: Vec::new(),
            cancel,
        }
    }

    /// Append one visible line to the captured output.
    pub fn line(&mut self, s: impl Into<String>) {
        self.lines.push(s.into());
    }

    /// True once the shared cancel token has been set (e.g. the user quit
    /// mid-run). Examples poll this in their loops and stop early.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    /// Borrow the shared cancel token, so the caller can set it.
    pub fn cancel(&self) -> &Arc<AtomicBool> {
        &self.cancel
    }

    /// Consume the sink and return every captured line, in order.
    pub fn take_lines(self) -> Vec<String> {
        self.lines
    }
}

/// A runnable spinlock-consumer example.
pub trait Example: Send {
    /// Stable display name (shown in the list and panels).
    fn name(&self) -> &'static str;

    /// One-line description (the list's secondary text).
    fn one_liner(&self) -> &'static str;

    /// The honest spinlock-vs-other-guards explanation (right-hand panel).
    fn explanation(&self) -> &'static str;

    /// The example's own source code, for the in-TUI code viewer.
    fn code(&self) -> &'static str;

    /// Object-safe clone, so the TUI can move an owned copy into the
    /// background thread while keeping the list intact.
    fn clone_box(&self) -> Box<dyn Example>;

    /// Run the example, writing visible lines through `out`.
    ///
    /// Returns `true` on completion, `false` if it stopped early because
    /// `out.is_cancelled()` became true. Must be bounded in iterations and
    /// must poll `out.is_cancelled()` periodically.
    fn run(&mut self, out: &mut ExampleOutput) -> bool;
}

/// All ten examples, in stable display order.
pub fn all() -> Vec<Box<dyn Example>> {
    vec![
        Box::new(short_critical_section::ShortCriticalSectionExample),
        Box::new(atomic_counter_tradeoff::AtomicCounterTradeoffExample),
        Box::new(read_mostly_cache::ReadMostlyCacheExample),
        Box::new(task_queue::TaskQueueExample),
        Box::new(no_sleep_context::NoSleepContextExample),
        Box::new(held_across_syscall::HeldAcrossSyscallExample),
        Box::new(sharded_locks::ShardedLocksExample),
        Box::new(bounded_ring_buffer::BoundedRingBufferExample),
        Box::new(vec_push::VecPushExample),
        Box::new(try_lock_fast_path::TryLockFastPathExample),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::RecvTimeoutError;
    use std::time::Duration;

    /// How long an example may run before its test is failed as hung.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Run a (cloned) example on a detached thread — exactly what the TUI
    /// does — and return the captured lines. The cancel token is left unset
    /// unless the example overruns [`TEST_TIMEOUT`], in which case it is set
    /// (so a cooperative example winds down) and the test fails.
    fn run_example_in_thread(example: &dyn Example) -> Vec<String> {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut out = ExampleOutput::new(Arc::clone(&cancel));
        let mut ex = example.clone_box();
        let name = ex.name();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _completed = ex.run(&mut out);
            let _ = tx.send(out.take_lines());
        });
        match rx.recv_timeout(TEST_TIMEOUT) {
            Ok(lines) => lines,
            Err(RecvTimeoutError::Timeout) => {
                cancel.store(true, Ordering::Relaxed);
                panic!("example {name:?} still running after {TEST_TIMEOUT:?}");
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("example {name:?} panicked before producing output");
            }
        }
    }

    #[test]
    fn all_returns_ten_examples() {
        let examples = all();
        assert_eq!(examples.len(), 10);
    }

    #[test]
    fn all_have_distinct_names() {
        let examples = all();
        let names: Vec<&str> = examples.iter().map(|e| e.name()).collect();
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len());
    }

    macro_rules! example_test {
        ($name:ident, $path:path, $expected:expr) => {
            #[test]
            fn $name() {
                let lines = run_example_in_thread(&$path);
                assert!(!lines.is_empty(), "example produced no output");
                let last = lines.last().unwrap();
                assert_eq!(
                    last, $expected,
                    "unexpected final line (all lines: {:?})",
                    lines
                );
            }
        };
    }

    example_test!(
        test_short_critical_section,
        short_critical_section::ShortCriticalSectionExample,
        "final counter = 2000"
    );
    example_test!(
        test_atomic_counter_tradeoff,
        atomic_counter_tradeoff::AtomicCounterTradeoffExample,
        "hits = 1000, misses = 1000, atomic = 1000 (all agree)"
    );
    example_test!(
        test_read_mostly_cache,
        read_mostly_cache::ReadMostlyCacheExample,
        "size = 2, lookups = 200, inserts = 2"
    );
    example_test!(
        test_task_queue,
        task_queue::TaskQueueExample,
        "produced = 100, consumed = 100"
    );
    example_test!(
        test_no_sleep_context,
        no_sleep_context::NoSleepContextExample,
        "no-sleep context processed 50 items, counter = 60"
    );
    example_test!(
        test_held_across_syscall,
        held_across_syscall::HeldAcrossSyscallExample,
        "holder released; contender serviced 3 times"
    );
    example_test!(
        test_sharded_locks,
        sharded_locks::ShardedLocksExample,
        "total = 100 across 8 shards [13, 13, 13, 13, 12, 12, 12, 12]"
    );
    example_test!(
        test_bounded_ring_buffer,
        bounded_ring_buffer::BoundedRingBufferExample,
        "produced = 100, consumed = 100, dropped = 0"
    );
    example_test!(
        test_vec_push,
        vec_push::VecPushExample,
        "contents (sorted) = [1, 2, 2]"
    );
    example_test!(
        test_try_lock_fast_path,
        try_lock_fast_path::TryLockFastPathExample,
        "total = 100 probes resolved"
    );
}
