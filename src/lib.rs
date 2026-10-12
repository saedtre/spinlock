//! A minimal, hand-rolled `SpinLock<T>` in the teaching-artifact spirit:
//! a single `AtomicBool` flag plus an `UnsafeCell<T>`, with a tight
//! `spin_loop` busy-wait. No backoff, no `thread::park`, no fairness
//! queueing, no poisoning — that is the point of the artifact.
//!
//! # API
//!
//! - [`SpinLock::new`] — build a lock around a value.
//! - [`SpinLock::lock`] — busy-wait until the lock is held; returns a [`Guard`].
//! - [`SpinLock::try_lock`] — non-blocking probe; `None` if the lock is already held.
//! - [`SpinLock::into_inner`] — consume the lock and return the value.
//! - `impl<T: Default> Default` — a lock around `T::default()`.
//!
//! There is deliberately **no** raw `get(&self) -> &T`: handing out a live
//! `&T` without holding the flag is an aliasing hazard, and a *locked* read
//! is already available via `lock().deref()`.
//!
//! The crate also ships ten runnable spinlock-consumer examples in
//! [`examples`], shared by the TUI binary and the test harness.

pub mod examples;

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};

/// A minimal spin lock: a single `AtomicBool` flag guarding an `UnsafeCell<T>`.
pub struct SpinLock<T> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

unsafe impl<T> Sync for SpinLock<T> where T: Send {}

impl<T> SpinLock<T> {
    /// Create a new spin lock around `value`.
    pub const fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    /// Block (busy-wait) until the lock is free, then hold it.
    ///
    /// On contention this tight loop burns a core; see the crate docs for
    /// the honest cost model behind that choice.
    pub fn lock(&self) -> Guard<'_, T> {
        while self.locked.swap(true, Acquire) {
            std::hint::spin_loop();
        }
        Guard { lock: self }
    }

    /// Non-blocking probe: acquire the lock and return `Some(guard)` if it
    /// was free, otherwise return `None` without waiting.
    ///
    /// This is the "is it free?" check that lets a fast path bail out (skip
    /// or defer work) instead of blocking — the natural companion to a
    /// no-sleep posture. `std::sync::Mutex::try_lock` offers the same shape;
    /// here it pairs with a lock that never sleeps by design.
    pub fn try_lock(&self) -> Option<Guard<'_, T>> {
        if self.locked.swap(true, Acquire) {
            None
        } else {
            Some(Guard { lock: self })
        }
    }

    /// Consume the lock and return the value.
    ///
    /// Safe: taking `self` by value implies the caller has exclusive access
    /// to the `SpinLock`, so no other thread can hold (or reach) the flag or
    /// the value at the same time. Useful when the lock is no longer needed.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: Default> Default for SpinLock<T> {
    /// A spin lock around `T::default()`.
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// RAII token returned by [`SpinLock::lock`] / [`SpinLock::try_lock`].
///
/// `Drop` releases the flag. Note this lock does **not** poison: a panic in
/// the critical section still runs `drop` and unlocks, so the lock is never
/// stuck — but the guarded data may be left inconsistent with no signal to
/// other threads.
pub struct Guard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // Safety: the very existence of this Guard
        // guarantees we've exclusively locked the lock.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // Safety: the very existence of this Guard
        // guarantees we've exclusively locked the lock.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn lock_serializes() {
        let x = SpinLock::new(Vec::new());
        std::thread::scope(|s| {
            s.spawn(|| x.lock().push(1));
            s.spawn(|| {
                let mut g = x.lock();
                g.push(2);
                g.push(2);
            });
        });
        let g = x.lock();
        let mut sorted = g.as_slice().to_vec();
        sorted.sort();
        assert_eq!(sorted, [1, 2, 2]);
    }

    #[test]
    fn try_lock_some_when_free_none_when_held() {
        let x = SpinLock::new(0i32);
        assert!(x.try_lock().is_some());
        // Hold a guard, then the probe must fail.
        let g = x.lock();
        assert!(x.try_lock().is_none());
        drop(g);
        assert!(x.try_lock().is_some());
    }

    #[test]
    fn into_inner_returns_value() {
        let x = SpinLock::new(42u8);
        assert_eq!(x.into_inner(), 42);
    }

    #[test]
    fn default_produces_default_value() {
        let x: SpinLock<Vec<u8>> = SpinLock::default();
        assert!(x.lock().is_empty());
    }

    #[test]
    fn drop_releases_flag() {
        let x = SpinLock::new(0i32);
        {
            let mut g = x.lock();
            *g = 7;
            assert!(x.try_lock().is_none());
        } // guard dropped here -> flag released
        assert!(x.try_lock().is_some());
        assert_eq!(*x.lock(), 7);
    }

    #[test]
    fn concurrent_increments_are_correct() {
        let counter = Arc::new(SpinLock::new(0i64));
        let threads = 8usize;
        let iterations = 1_000usize;
        std::thread::scope(|s| {
            for _ in 0..threads {
                let counter = Arc::clone(&counter);
                s.spawn(move || {
                    for _ in 0..iterations {
                        *counter.lock() += 1;
                    }
                });
            }
        });
        assert_eq!(*counter.lock(), (threads * iterations) as i64);
    }
}
