//! Module `spinlock` implement read-write-spinlock, useful for non-blocking concurrency.
//!
//! Blocking concurrency can have impact on latency. When operations that require
//! [rw-exclusion][rw-lock] is going to be quick and short, we can use non-blocking
//! primitives like latch-and-spin.
//!
//! **What is Latch and spin ?**
//!
//! In typical multi-core processors, concurrent read operations are always safe and
//! consistent. But it becomes in-consistent, when there is a writer concurrently
//! modifying data while readers are loading it from memory.
//!
//! Latch-and-lock mechanism can be used when we want to allow one or more concurrent
//! writer(s) along with readers.
//!
//! Imagine a door leading into a room. This door has some special properties:
//!
//! 1. The door has a latch and a lock.
//! 2. By default the door is unlocked and open.
//! 3. A **reader** can enter the room only when the door is open. If it is latched or
//!    locked, they must wait.
//! 4. A **writer** can enter the room only when the door is un-locked and, un-latched
//!    and, there are no other **reader** or **writer** in the room.
//! 5. A **writer** shall spin, until it can latch the door, which can happen only
//!    when no other writer has already latched it.
//! 6. Once the door is latched by a **writer**, no other **writer** or **reader** can
//!    enter the room because of (3) and (4) properties. But all **readers** who are
//!    already inside the room can finish their job and then exit.
//! 7. A **writer** can enter the room only after locking the door, which the
//!    **writer** can do only after all the **readers** have exited.
//! 8. When trying to acquire read-permission or write-permission, the caller thread
//!    shall spin until all the conditions from (3) to (7) are met. Once a thread
//!    acquires necessary permission it can continue to finish its job and then release
//!    the permission.
//!
//! [rw-lock]: https://en.wikipedia.org/wiki/Readers–writer_lock

#[cfg(feature = "debug")]
use log::debug;

#[cfg(test)]
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use std::{fmt, result};

#[cfg(test)]
use crate::Result;

// TODO: Experiment with different atomic::Ordering to improve performance.
// TODO: experiment with thread::yield_now().

/// Spinlock implements latch-and-spin mechanism for non-blocking
/// concurrency.
///
/// It uses AtomicU32 for:
/// * ref-count, bits [0-61].
/// * latch flag, bit 62.
/// * lock flag, bit 63.
pub struct Spinlock<T> {
    latchlock: AtomicU32,
    read_locks: AtomicU32,
    write_locks: AtomicU32,
    conflicts: AtomicU32,

    value: T,
}

#[cfg(feature = "debug")]
impl<T> Drop for Spinlock<T> {
    fn drop(&mut self) {
        debug!("{}", self.to_stats().unwrap());
    }
}

impl<T> Spinlock<T> {
    const LATCH_FLAG: u32 = 0x40000000;
    const LOCK_FLAG: u32 = 0x80000000;
    const LATCH_LOCK_FLAG: u32 = 0xC0000000;
    const READERS_FLAG: u32 = 0x3FFFFFFF;

    /// Create a new Spinlock
    pub fn new(value: T) -> Spinlock<T> {
        Spinlock {
            latchlock: AtomicU32::new(0),
            read_locks: AtomicU32::new(0),
            write_locks: AtomicU32::new(0),
            conflicts: AtomicU32::new(0),

            value,
        }
    }

    /// Acquire latch for read permission.
    pub fn read(&self) -> ReadGuard<T> {
        loop {
            let old = self.latchlock.load(SeqCst);
            if (old & Self::LATCH_LOCK_FLAG) == 0 {
                // latch is not acquired by a writer
                if self.latchlock.compare_exchange(old, old + 1, SeqCst, SeqCst).is_ok() {
                    if cfg!(feature = "debug") {
                        self.read_locks.fetch_add(1, SeqCst);
                    }
                    break ReadGuard { door: self };
                }
            }
            if cfg!(feature = "debug") {
                self.conflicts.fetch_add(1, SeqCst);
            }
        }
    }

    /// Acquire latch & lock for write permission.
    pub fn write(&self) -> WriteGuard<T> {
        loop {
            let old = self.latchlock.load(SeqCst);
            if (old & Self::LATCH_FLAG) == 0 {
                // latch is not acquired by a writer
                if (old & Self::LOCK_FLAG) != 0 {
                    panic!(concat!(
                        "if latch is flipped-off, lock can't be flipped-on! ",
                        "call the programmer"
                    ));
                }
                let new = old | Self::LATCH_FLAG;
                if self.latchlock.compare_exchange(old, new, SeqCst, SeqCst).is_ok() {
                    break;
                }
            }
            if cfg!(feature = "debug") {
                self.conflicts.fetch_add(1, SeqCst);
            }
        }
        // acquire lock
        loop {
            let old = self.latchlock.load(SeqCst);
            if (old & Self::READERS_FLAG) == 0 {
                let new = old | Self::LOCK_FLAG;
                if self.latchlock.compare_exchange(old, new, SeqCst, SeqCst).is_ok() {
                    if cfg!(feature = "debug") {
                        self.write_locks.fetch_add(1, SeqCst);
                    }
                    let door = unsafe {
                        let door = self as *const Self as *mut Self;
                        door.as_mut().unwrap()
                    };
                    break WriteGuard { door };
                }
                panic!(concat!(
                    "latch is acquired, ZERO readers, but unable to lock! ",
                    "call the programmer"
                ));
            }
            if cfg!(feature = "debug") {
                self.conflicts.fetch_add(1, SeqCst);
            }
        }
    }

    #[cfg(test)]
    pub fn to_stats(&self) -> Result<Stats> {
        let rl = usize::try_from(self.read_locks.load(SeqCst))?;
        let wl = usize::try_from(self.write_locks.load(SeqCst))?;
        let cn = usize::try_from(self.conflicts.load(SeqCst))?;
        Ok(Stats {
            latchlock: self.latchlock.load(SeqCst),
            read_locks: rl,
            write_locks: wl,
            conflicts: cn,
        })
    }
}

/// Type to handle read-latch, when latchlock gets dropped the latch is released.
pub struct ReadGuard<'a, T> {
    door: &'a Spinlock<T>,
}

impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.door.value
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        self.door.latchlock.fetch_sub(1, SeqCst);
    }
}

/// Type to handle write-latch, when latchlock gets dropped the latch is released.
pub struct WriteGuard<'a, T> {
    door: &'a mut Spinlock<T>,
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.door.value
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.door.value
    }
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let old = self.door.latchlock.load(SeqCst);
        if (old & Spinlock::<T>::READERS_FLAG) > 0 {
            panic!(concat!(
                "can't have active readers, when lock is held! ",
                "call the programmer"
            ));
        }
        if self.door.latchlock.compare_exchange(old, 0, SeqCst, SeqCst).is_err() {
            panic!(concat!(
                "cant' have readers/writers to modify when locked! ",
                "call the programmer"
            ))
        }
    }
}

/// Statistic type, to capture [Spinlock] metrics.
#[derive(Default)]
pub struct Stats {
    /// Actual 64-bit latchlock of the Spinlock when
    /// [to_stats][Spinlock::to_stats] is called.
    pub latchlock: u32,
    /// Total number of read locks so far.
    pub read_locks: usize,
    /// Total number of write locks so far.
    pub write_locks: usize,
    /// Total number of conflicts so far, while acquire the latch.
    pub conflicts: usize,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(
            f,
            concat!(
                "{{ latchlock = {:X}, read_locks = {}, ",
                "write_locks = {}, conflicts = {} }}",
            ),
            self.latchlock, self.read_locks, self.write_locks, self.conflicts,
        )
    }
}

#[cfg(test)]
#[path = "spinlock_test.rs"]
mod spinlock_test;
