# spinlock

A minimal, hand-rolled `SpinLock<T>` (one `AtomicBool` flag plus an
`UnsafeCell<T>`, with a tight `spin_loop` busy-wait) and a terminal UI that
lets you browse and run ten examples of code that would typically consume a
spinlock. Each example comes with a short, honest explanation of how a
spinlock compares with the other concurrency guards (`Mutex`, `RwLock`,
atomics, channels) for that pattern, including the cases where it loses.

## How to run

```sh
cargo run
```

This opens the TUI: the example list on the left, the selected example's
explanation (or its source) on the right, a live picture of the run's traffic
through the lock below them, and the run's output at the bottom. The selected
example runs in-process on a background thread, so the interface stays
responsive; its output lines appear as they are written, not when it ends.

| Key | Action |
|---|---|
| `Up` / `k` | Select the previous example (wraps) |
| `Down` / `j` | Select the next example (wraps) |
| `Enter` / `r` | Run the selected example |
| `c` | Toggle the right-hand panel between the explanation and the example's source code |
| `PageUp` / `PageDown` | Scroll the right-hand panel |
| `[` / `]` | Scroll the output pane back towards a run's first line / forward to its newest |
| `q` / `Esc` | Quit (a run in progress is asked to cancel first) |

`Ctrl-C` also quits. The terminal is restored on exit, including after a panic.

When the explanation or the source is longer than its panel, the panel's
bottom border says so (`rows 1-10 of 13 (PgUp/PgDn)`). The output pane does
the same when a run has written more than fits
(`lines 4-9 of 9 ([ back, ] forward)`); it follows the newest line until you
scroll back.

The TUI needs an interactive terminal. Started without one (from a script, or
with its output piped) it prints a one-line message and exits with status 1.

### The "Through the spinlock" picture

Every example reports what its threads do with the lock, and the TUI redraws
that about twenty times a second:

```
value lock: HELD by holder   ->   value = 0
holder       HOLD |██████████████████████|     1 acq
contender 1  SPIN |░░░░░░░░░░░░░░░░░░░░░░|     0 acq  1 busy
contender 2  SPIN |░░░░░░░░░░░░░░░░░░░░░░|     0 acq  1 busy
through the lock  |░░░░░░░░░░░░░░░░░░░░░░| 0/9 increments
```

- The first line is the lock (free, or who holds it) and the data it guards.
- Then one line per thread: what it is doing now (`idle`, `SPIN` =
  busy-waiting, `HOLD` = inside the critical section, `done`), how many times
  it has acquired the lock, how many times it found the lock `busy`, and the
  total time it has `spun` waiting. A thread whose share of the work is not
  known in advance (the queue's consumers, the no-sleep contexts) has no bar,
  only its counts.
- The last line is how much of the example's work has passed through the lock.

Two things to know when reading it:

- The state column is a sample. A critical section of a few nanoseconds is
  almost never caught as `HOLD`; the counters are exact.
- So that a run can be watched, the TUI asks each example to pause briefly
  between operations (a run takes one to two seconds). The pauses are outside
  the critical sections, except in examples 6 and 10, whose point is a holder
  that stays in the lock for a long time. In examples 1 to 3 the threads
  carry on together after each pause, so they still meet at the lock and the
  `busy` counts show it. The tests run the same code with the pauses switched
  off.

## What each example shows

| # | Example | What it shows |
|---|---|---|
| 1 | Short critical section | A few-nanosecond increment under the lock: the case where spinning beats a contended `Mutex`'s kernel round-trip. |
| 2 | Counter: guard vs atomic (honest) | A two-field stats struct under the lock next to a plain `AtomicI64`: the atomic wins for one integer, the guard wins when several fields must move together. |
| 3 | Read-mostly cache (honest) | A `HashMap` cache behind one lock: a single flag serializes readers too, so `RwLock` is usually the better choice. |
| 4 | Shared task queue | One producer and several consumers sharing a locked `VecDeque`, where an `mpsc` channel's single receiver is awkward. |
| 5 | No-sleep context (the canonical reason) | Contexts that may not park or sleep (interrupt, real-time, signal style): busy-waiting is the only legal wait. |
| 6 | Held across a syscall (when NOT to use) | The anti-example: a lock held across a blocking operation makes every contender burn a core; use a sleeping `Mutex`. |
| 7 | Sharded / per-key locking | Eight independent locks addressed by key, next to one global lock: sharding cuts contention, and a light guard suits the short per-shard section. |
| 8 | Fixed-size ring buffer | A bounded, pre-allocated ring: no heap work behind the lock and a tiny push/pop section. |
| 9 | Vec push (original demo) | The crate's original smoke test, relocated: two threads push into a locked `Vec`, a heap-allocating section that actually favors a sleeping lock. |
| 10 | Fast-path check-then-bail (try_lock) | A non-blocking `try_lock()` probe that takes a fallback path instead of waiting when the lock is held. |

The examples live in `src/examples/`, one file per example, and each one has
an automated test (`cargo test`). `src/examples/viz.rs` holds the bookkeeping
behind the picture.

## Library

The lock is exposed by the library target:

```rust
use spinlock::SpinLock;

let lock = SpinLock::new(Vec::new());
lock.lock().push(1);                  // busy-waits until the lock is held
if let Some(mut guard) = lock.try_lock() {
    guard.push(2);                    // non-blocking probe; None if held
}
let values = lock.into_inner();       // consume the lock, get the value back
```

- `SpinLock::new(value)` builds a lock around a value (`const fn`).
- `lock()` busy-waits until the lock is held and returns a `Guard`; dropping
  the guard unlocks.
- `try_lock()` returns `Some(Guard)` if the lock was free and `None` otherwise,
  without waiting.
- `into_inner()` consumes the lock and returns the value.

`SpinLock<T>` also implements `Default` when `T: Default`. Like
`std::sync::MutexGuard`, a `Guard` can be shared between threads only when
`T: Sync`.

## Caveats

This is a teaching artifact, not a production lock:

- **Busy-waiting burns a core.** A contended `lock()` spins at 100% CPU until
  the holder releases. On a single core this is the worst case.
- **No backoff, no parking.** The loop never yields or sleeps, however long
  the wait.
- **No fairness.** There is no queue; a waiter can be starved under heavy
  contention.
- **No poisoning.** If a critical section panics, the guard still unlocks, but
  the data may be left inconsistent and other threads get no signal.
- The performance statements in the explanations follow the usual
  spin-versus-futex cost model; they are not benchmarked in this repository.

## Build note (macOS with a non-Apple `cc` first on `PATH`)

If linking fails with `ld: library not found for -liconv`, the first `cc` on
your `PATH` is not Apple's (for example a Nix GCC). Either fix the shell's
toolchain, or point cargo at Apple's linker with a local `.cargo/config.toml`:

```toml
[target.aarch64-apple-darwin]
linker = "/usr/bin/cc"
```

If the linker then cannot read the SDK that `xcrun` selects, also pin
`SDKROOT` under `[env]` in the same file. That file describes one machine, so `.gitignore` lists it and it is not part
of the repository.
