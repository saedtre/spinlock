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
//! - it **describes itself to the live picture** with
//!   [`ExampleOutput::visualise`] and reports its lock traffic through the
//!   returned [`Viz`] (see [`viz`]), pausing only via [`Viz::pause`];
//! - it **always emits a deterministic final summary line** that the tests
//!   assert on (variable / interleaved lines are informational only).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

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
pub mod viz;

pub use viz::{LaneState, Viz, VizSnapshot, VizSpec};

/// Output sink for a running example: its visible lines, a live picture of
/// its traffic through the lock, and a cooperative cancel token.
///
/// Every line written via [`Self::line`] is kept (for [`Self::take_lines`])
/// and, if the sink was built with [`Self::streaming`], also sent at once to
/// the receiver — that is how the TUI shows output while the example is
/// still running. The picture ([`Viz`]) is shared the same way: the caller
/// keeps a clone from [`Self::viz`] and samples it during the run. `cancel`
/// is shared via an [`Arc`] so the surrounding event loop can ask a running
/// example to stop.
pub struct ExampleOutput {
    lines: Vec<String>,
    cancel: Arc<AtomicBool>,
    sink: Option<Sender<String>>,
    viz: Viz,
}

impl ExampleOutput {
    /// Create a fresh output sink that shares the given cancel token. Lines
    /// are only captured (no live receiver) and the run is not paced.
    pub fn new(cancel: Arc<AtomicBool>) -> Self {
        Self {
            lines: Vec::new(),
            viz: Viz::new(Arc::clone(&cancel), Duration::ZERO),
            cancel,
            sink: None,
        }
    }

    /// Like [`Self::new`], but every line is also delivered to the returned
    /// receiver the moment it is written, and the example is asked to pace
    /// itself in units of `pace` so a person can watch it
    /// (`Duration::ZERO` = full speed).
    pub fn streaming(cancel: Arc<AtomicBool>, pace: Duration) -> (Self, Receiver<String>) {
        let (tx, rx) = mpsc::channel();
        let out = Self {
            lines: Vec::new(),
            viz: Viz::new(Arc::clone(&cancel), pace),
            cancel,
            sink: Some(tx),
        };
        (out, rx)
    }

    /// Append one visible line to the captured output (and stream it, if a
    /// live receiver is attached).
    pub fn line(&mut self, s: impl Into<String>) {
        let s = s.into();
        if let Some(sink) = &self.sink {
            // The receiver going away (the TUI quit) is not the example's
            // problem; the line is still captured below.
            let _ = sink.send(s.clone());
        }
        self.lines.push(s);
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

    /// A handle to this run's picture, for the caller to sample.
    pub fn viz(&self) -> Viz {
        self.viz.clone()
    }

    /// Describe the example to the picture (resetting it) and get the handle
    /// the example's threads report through.
    pub fn visualise(&mut self, spec: VizSpec) -> Viz {
        self.viz.configure(spec);
        self.viz.clone()
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
    use std::time::Instant;

    /// How long an example may run before its test is failed as hung.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// What one finished run produced.
    struct Run {
        completed: bool,
        lines: Vec<String>,
        picture: VizSnapshot,
    }

    /// Run a (cloned) example on a detached thread — exactly what the TUI
    /// does — and return what it produced. If the example overruns
    /// [`TEST_TIMEOUT`] the cancel token is set (so a cooperative example
    /// winds down) and the test fails.
    fn run_with(example: &dyn Example, cancel: Arc<AtomicBool>, mut out: ExampleOutput) -> Run {
        let viz = out.viz();
        let mut ex = example.clone_box();
        let name = ex.name();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let completed = ex.run(&mut out);
            let _ = tx.send((completed, out.take_lines()));
        });
        match rx.recv_timeout(TEST_TIMEOUT) {
            Ok((completed, lines)) => Run {
                completed,
                lines,
                picture: viz.snapshot(),
            },
            Err(RecvTimeoutError::Timeout) => {
                cancel.store(true, Ordering::Relaxed);
                panic!("example {name:?} still running after {TEST_TIMEOUT:?}");
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("example {name:?} panicked before producing output");
            }
        }
    }

    /// An ordinary, uncancelled, unpaced run.
    fn run_example_in_thread(example: &dyn Example) -> Run {
        let cancel = Arc::new(AtomicBool::new(false));
        let out = ExampleOutput::new(Arc::clone(&cancel));
        run_with(example, cancel, out)
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

    #[test]
    fn line_is_streamed_the_moment_it_is_written() {
        let cancel = Arc::new(AtomicBool::new(false));
        let (mut out, rx) = ExampleOutput::streaming(cancel, Duration::ZERO);
        assert!(rx.try_recv().is_err());
        out.line("first");
        // Delivered while the sink (the "run") is still alive.
        assert_eq!(rx.try_recv().as_deref(), Ok("first"));
        out.line("second");
        assert_eq!(rx.try_recv().as_deref(), Ok("second"));
        assert_eq!(out.take_lines(), ["first", "second"]);
    }

    #[test]
    fn line_survives_a_dropped_receiver() {
        let cancel = Arc::new(AtomicBool::new(false));
        let (mut out, rx) = ExampleOutput::streaming(cancel, Duration::ZERO);
        drop(rx);
        out.line("still captured");
        assert_eq!(out.take_lines(), ["still captured"]);
    }

    /// The streaming contract end to end: a running example's first lines
    /// reach the receiver before the example has finished.
    #[test]
    fn output_arrives_while_the_example_is_still_running() {
        let cancel = Arc::new(AtomicBool::new(false));
        let (mut out, rx) = ExampleOutput::streaming(Arc::clone(&cancel), Duration::ZERO);
        let finished = Arc::new(AtomicBool::new(false));
        let handle = std::thread::spawn({
            let finished = Arc::clone(&finished);
            move || {
                // ~100 ms: the holder sleeps 5 x 20 ms after the intro lines.
                let completed = held_across_syscall::HeldAcrossSyscallExample.run(&mut out);
                finished.store(true, Ordering::Release);
                completed
            }
        });
        let first = rx.recv_timeout(TEST_TIMEOUT).expect("a first line");
        assert!(
            !finished.load(Ordering::Acquire),
            "the run had already ended when its first line arrived"
        );
        assert!(first.starts_with("the holder will sleep"), "{first:?}");
        assert!(handle.join().expect("no panic"));
        // The channel closes when the run ends, having carried every line.
        let rest: Vec<String> = rx.iter().collect();
        assert!(rest.last().unwrap().starts_with("holder released; final value = 9"));
    }

    /// Every example gives up, reporting `false`, when it is cancelled
    /// before it starts.
    #[test]
    fn every_example_returns_false_when_cancelled_up_front() {
        for example in all() {
            let cancel = Arc::new(AtomicBool::new(true));
            let out = ExampleOutput::new(Arc::clone(&cancel));
            let run = run_with(example.as_ref(), cancel, out);
            assert!(!run.completed, "{:?} claimed completion", example.name());
        }
    }

    /// A paced (TUI-speed) run lasts around a second; cancelling it must
    /// still stop it promptly, pauses included.
    #[test]
    fn every_paced_example_stops_soon_after_a_mid_run_cancel() {
        for example in all() {
            let cancel = Arc::new(AtomicBool::new(false));
            let (mut out, _rx) =
                ExampleOutput::streaming(Arc::clone(&cancel), Duration::from_millis(4));
            let mut ex = example.clone_box();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(ex.run(&mut out));
            });
            std::thread::sleep(Duration::from_millis(30));
            let cancelled_at = Instant::now();
            cancel.store(true, Ordering::Relaxed);
            let completed = rx
                .recv_timeout(TEST_TIMEOUT)
                .unwrap_or_else(|e| panic!("{:?} did not stop: {e}", example.name()));
            assert!(!completed, "{:?} ran to completion", example.name());
            assert!(
                cancelled_at.elapsed() < Duration::from_millis(500),
                "{:?} took {:?} to stop",
                example.name(),
                cancelled_at.elapsed()
            );
        }
    }

    /// A paced run reaches the same final line as an unpaced one.
    #[test]
    fn paced_run_and_rerun_give_the_same_result() {
        let example = vec_push::VecPushExample;
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, _rx) = ExampleOutput::streaming(Arc::clone(&cancel), Duration::from_micros(100));
        let paced = run_with(&example, cancel, out);
        let again = run_example_in_thread(&example);
        assert!(paced.completed && again.completed);
        assert_eq!(paced.lines.last(), again.lines.last());
        assert_eq!(again.lines.last().unwrap(), "contents (sorted) = [1, 2, 2]");
    }

    /// Every example also runs to completion when paced (the way the TUI
    /// runs it, with pauses and gates in play) and ends on the same line.
    #[test]
    fn every_paced_example_completes_with_the_same_final_line() {
        for example in all() {
            let cancel = Arc::new(AtomicBool::new(false));
            let (out, _rx) =
                ExampleOutput::streaming(Arc::clone(&cancel), Duration::from_micros(100));
            let paced = run_with(example.as_ref(), cancel, out);
            let unpaced = run_example_in_thread(example.as_ref());
            let name = example.name();
            assert!(paced.completed && unpaced.completed, "{name:?}");
            assert_eq!(paced.lines.last(), unpaced.lines.last(), "{name:?}");
        }
    }

    /// The picture every example draws is complete at the end of a run:
    /// all lanes done, nobody holding, and all the expected work through.
    #[test]
    fn every_example_feeds_its_whole_workload_through_the_picture() {
        for example in all() {
            let run = run_example_in_thread(example.as_ref());
            let name = example.name();
            let p = &run.picture;
            assert!(run.completed, "{name:?}");
            assert!(!p.is_empty(), "{name:?} drew no picture");
            assert!(p.target > 0, "{name:?}");
            for lane in &p.lanes {
                assert_eq!(lane.state, LaneState::Done, "{name:?} lane {:?}", lane.label);
                assert!(lane.acquired >= 1, "{name:?} lane {:?}", lane.label);
            }
            // try_lock is the one example where work is *meant* to bypass
            // the lock: fast-path hits plus the probers' misses make 100.
            let bypassed: u64 = if name.contains("try_lock") {
                p.lanes[1..].iter().map(|l| l.busy).sum()
            } else {
                0
            };
            assert_eq!(p.fed + bypassed, p.target, "{name:?}: {p:?}");
            assert_eq!(p.render(78).len(), p.lanes.len() + 2, "{name:?}");
        }
    }

    macro_rules! example_test {
        ($name:ident, $path:path, $expected:expr) => {
            #[test]
            fn $name() {
                let run = run_example_in_thread(&$path);
                assert!(run.completed, "example did not complete");
                let last = run.lines.last().expect("example produced no output");
                assert_eq!(
                    last, $expected,
                    "unexpected final line (all lines: {:?})",
                    run.lines
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
        "holder released; final value = 9 (1 held + 2 contended + 6 uncontended increments)"
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
        "total = 100 probes resolved, value consistent"
    );

    /// The anti-example must show its cost: each contender busy-waits for
    /// (nearly) the whole 100 ms the holder sleeps with the lock.
    #[test]
    fn held_across_syscall_contenders_spin_for_the_whole_hold() {
        let run = run_example_in_thread(&held_across_syscall::HeldAcrossSyscallExample);
        for lane in &run.picture.lanes[1..] {
            // At least the phase-1 wait; the two contenders may also bump
            // into each other during their uncontended phase-2 increments.
            assert!(lane.busy >= 1, "{lane:?}");
            assert!(lane.spun >= Duration::from_millis(50), "{lane:?}");
        }
        assert!(
            run.lines.iter().any(|l| l.starts_with("contender 1 busy-waited")),
            "{:?}",
            run.lines
        );
    }

    /// The no-sleep contexts must really poll: the device becomes ready
    /// 2 ms after both are waiting, so each busy-waits at least about that.
    #[test]
    fn no_sleep_context_actually_spins_before_ready() {
        let run = run_example_in_thread(&no_sleep_context::NoSleepContextExample);
        for lane in &run.picture.lanes {
            assert!(lane.spun >= Duration::from_millis(1), "{lane:?}");
        }
        assert!(
            !run.lines.iter().any(|l| l.contains("made 0 spin_loop() polls")),
            "{:?}",
            run.lines
        );
    }

    /// The queue's producer and consumers each did their share through the
    /// lock: 100 pushes by the producer, 100 successful pops in total.
    #[test]
    fn task_queue_producer_and_consumers_share_the_lock() {
        let run = run_example_in_thread(&task_queue::TaskQueueExample);
        let lanes = &run.picture.lanes;
        assert_eq!(lanes[0].acquired, 100);
        assert!(lanes[1..].iter().map(|l| l.acquired).sum::<u64>() >= 100);
    }
}
