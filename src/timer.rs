//! Module implement differential timer.

use std::{mem, time};

/// Trait to be implemented by values that are managed by [Timer].
pub trait TimeoutValue {
    /// Call this to mark value as deleted. Once it is marked as deleted,
    /// [Timer::expired] won't return this value when it expires. On the other hand,
    /// it will be quitely deleted by `GC`.
    fn delete(&self);

    /// Return whether this value was marked as deleted.
    fn is_deleted(&self) -> bool;
}

/// Differential Timer, to add timers for messages, sessions etc and manage expiry.
///
/// * log(n) complexity for adding new timeouts.
/// * log(1) complexity for other operations.
///
/// Application shall call [Timer::gc] and [Timer::expired] periodically. If application
/// have no logic to call [TimeoutValue::delete] on the timer-entry, then there is no
/// need to call [Timer::gc].
pub struct Timer<T> {
    instant: time::Instant,
    head: Box<Titem<T>>,
}

enum Titem<T> {
    Head {
        next: Box<Titem<T>>,
    },
    Timeout {
        delta: u64,
        value: T,
        next: Box<Titem<T>>,
    },
    Sentinel,
}

impl<T> Default for Timer<T> {
    fn default() -> Timer<T> {
        Timer {
            instant: time::Instant::now(),
            head: Box::new(Titem::Head { next: Box::new(Titem::Sentinel) }),
        }
    }
}

impl<T> Timer<T> {
    /// Add a new timer entry, timer entry shall expire after `secs` seconds.
    pub fn add_timeout(&mut self, secs: u64, value: T) {
        let micros = (secs as u64) * 1_000_000;
        let mut ndelta = micros.saturating_sub(self.instant.elapsed().as_micros() as u64);

        let mut prev = self.head.as_mut();
        loop {
            match prev.take_next() {
                n @ Titem::Sentinel => {
                    let next = Titem::Timeout { delta: ndelta, value, next: Box::new(n) };
                    prev.set_next(next);
                    break;
                }
                mut nn @ Titem::Timeout { .. } if ndelta < nn.to_delta() => {
                    nn.differential(ndelta);
                    let delta = ndelta;
                    let next = Titem::Timeout { delta, value, next: Box::new(nn) };
                    prev.set_next(next);
                    break;
                }
                nn @ Titem::Timeout { .. } => {
                    ndelta = ndelta - nn.to_delta();
                    prev.set_next(nn);
                    prev = prev.as_mut_next();
                }
                Titem::Head { .. } => unreachable!(),
            }
        }
    }

    /// Return an iterator of all expired timer entries. Returned entries shall be
    /// removed from this timer-list. Pass None for `elapsed`.
    pub fn expired(&mut self, elapsed: Option<u64>) -> impl Iterator<Item = T>
    where
        T: Clone + TimeoutValue,
    {
        let micros = elapsed.unwrap_or(self.instant.elapsed().as_micros() as u64);
        self.instant += time::Duration::from_micros(micros);

        let mut expired = Vec::new();

        loop {
            match self.head.take_next() {
                Titem::Sentinel => {
                    self.head.set_next(Titem::Sentinel);
                    break;
                }
                Titem::Timeout { delta, value, next } if delta > micros => {
                    let delta = delta - micros;
                    let next = Titem::Timeout { delta, value, next };
                    self.head.set_next(next);
                    break;
                }
                Titem::Timeout { value, next, .. } if value.is_deleted() => {
                    // quitely remove and ignore this item.
                    self.head.set_next(*next);
                }
                Titem::Timeout { value, next, .. } => {
                    // exipired, remove and return this item.
                    expired.push(value);
                    self.head.set_next(*next);
                }
                Titem::Head { .. } => unreachable!(),
            }
        }

        self.gc();

        expired.into_iter()
    }

    /// Garbage collect all timer-entries marked as deleted by application.
    fn gc(&mut self)
    where
        T: TimeoutValue,
    {
        let mut prev = self.head.as_mut();
        loop {
            match prev.take_next() {
                next @ Titem::Sentinel => {
                    prev.set_next(next);
                    break;
                }
                Titem::Timeout { value, mut next, .. } if value.is_deleted() => {
                    let next = mem::replace(&mut next, Box::new(Titem::Sentinel));
                    prev.set_next(*next);
                }
                n @ Titem::Timeout { .. } => {
                    prev.set_next(n);
                    prev = match prev {
                        Titem::Timeout { next, .. } => next.as_mut(),
                        _ => unreachable!(),
                    };
                }
                Titem::Head { .. } => unreachable!(),
            }
        }
    }
}

impl<T> Titem<T> {
    fn differential(&mut self, ndelta: u64) {
        match self {
            Titem::Timeout { delta, .. } => *delta = *delta - ndelta,
            _ => unreachable!(),
        }
    }

    fn as_mut_next(&mut self) -> &mut Titem<T> {
        match self {
            Titem::Head { next } => next.as_mut(),
            Titem::Timeout { next, .. } => next.as_mut(),
            _ => unreachable!(),
        }
    }

    fn to_delta(&self) -> u64 {
        match self {
            Titem::Timeout { delta, .. } => *delta,
            _ => unreachable!(),
        }
    }

    fn take_next(&mut self) -> Titem<T> {
        match self {
            Titem::Head { next } => *mem::replace(next, Box::new(Titem::Sentinel)),
            Titem::Timeout { next, .. } => *mem::replace(next, Box::new(Titem::Sentinel)),
            _ => unreachable!(),
        }
    }

    fn set_next(&mut self, new_next: Titem<T>) -> Box<Titem<T>> {
        match self {
            Titem::Head { next } => mem::replace(next, Box::new(new_next)),
            Titem::Timeout { next, .. } => mem::replace(next, Box::new(new_next)),
            _ => unreachable!(),
        }
    }
}

#[cfg(any(feature = "fuzzy", test))]
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
#[cfg(any(feature = "fuzzy", test))]
use std::sync::Arc;

#[cfg(any(feature = "fuzzy", test))]
pub struct TimerEntry {
    pub value: u32,
    pub secs: u64,
    pub deleted: AtomicBool,
}

#[cfg(any(feature = "fuzzy", test))]
impl TimeoutValue for Arc<TimerEntry> {
    fn delete(&self) {
        self.deleted.store(true, SeqCst)
    }

    fn is_deleted(&self) -> bool {
        self.deleted.load(SeqCst)
    }
}
