//! Module implement differential timer.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::{collections::BTreeMap, fmt, mem, result, sync::Arc, time};

use crate::Result;

/// Differential Timer, to add timers for messages, sessions etc and manage expiry.
///
/// * log(n) complexity for adding new timeouts.
/// * log(1) complexity for other operations.
///
/// Application shall call [Timer::gc] and [Timer::expired] periodically. If application
/// have no logic to call [Timer::delete] on the timer-entry, then there is no
/// need to call [Timer::gc].
pub struct Timer<K, T> {
    instant: time::Instant,
    head: Box<Titem<K, T>>,
    entries: BTreeMap<K, Arc<TimerEntry<T>>>,
}

enum Titem<K, T> {
    Head {
        next: Box<Titem<K, T>>,
    },
    Timeout {
        delta: u64,
        key: K,
        te: Arc<TimerEntry<T>>,
        next: Box<Titem<K, T>>,
    },
    Sentinel,
}

impl<K, T> Default for Timer<K, T> {
    fn default() -> Timer<K, T> {
        Timer {
            instant: time::Instant::now(),
            head: Box::new(Titem::Head { next: Box::new(Titem::Sentinel) }),
            entries: BTreeMap::default(),
        }
    }
}

impl<K, T> Timer<K, T> {
    /// Add a new timer entry, timer entry shall expire after `secs` seconds.
    pub fn add_timeout(&mut self, secs: u64, key: K, value: T)
    where
        K: Ord + Clone,
    {
        let te = Arc::new(TimerEntry { value, secs, deleted: AtomicBool::new(false) });
        let micros = (secs as u64) * 1_000_000;
        let mut ndelta = micros.saturating_sub(self.instant.elapsed().as_micros() as u64);

        let mut prev = self.head.as_mut();
        loop {
            match prev.take_next() {
                n @ Titem::Sentinel => {
                    let key = key.clone();
                    let next = Titem::Timeout {
                        delta: ndelta,
                        key,
                        te: Arc::clone(&te),
                        next: Box::new(n),
                    };
                    prev.set_next(next);
                    break;
                }
                mut nn @ Titem::Timeout { .. } if ndelta < nn.to_delta() => {
                    nn.differential(ndelta);
                    let delta = ndelta;
                    let next = Titem::Timeout {
                        delta,
                        key: key.clone(),
                        te: Arc::clone(&te),
                        next: Box::new(nn),
                    };
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
        self.entries.insert(key, te);
    }

    /// Mark the entry specified by `key` as deleted.
    pub fn delete(&mut self, key: &K) -> Result<()>
    where
        K: Ord,
    {
        if let Some(te) = self.entries.remove(key) {
            te.delete();
        }
        Ok(())
    }

    /// Return an iterator of all expired timer entries. Returned entries shall be
    /// removed from this timer-list. Also deleted entries are not considered as expired
    /// and shall not be returned, they are returned only by gc().
    /// Pass None for `elapsed`.
    pub fn expired(&mut self, elapsed: Option<u64>) -> impl Iterator<Item = T>
    where
        K: Ord,
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
                Titem::Timeout { delta, key, te, next } if delta > micros => {
                    let delta = delta - micros;
                    let next = Titem::Timeout { delta, key, te, next };
                    self.head.set_next(next);
                    break;
                }
                Titem::Timeout { key, te, next, .. } => {
                    mem::drop(self.entries.remove(&key));
                    match Arc::try_unwrap(te) {
                        Ok(te) => expired.push(te.value),
                        Err(_) => unreachable!("fatal"),
                    }
                    self.head.set_next(*next);
                }
                Titem::Head { .. } => unreachable!(),
            }
        }

        expired.into_iter()
    }

    pub fn contains(&self, key: &K) -> bool
    where
        K: Ord,
    {
        self.entries.contains_key(key)
    }

    pub fn values(&self) -> Vec<T>
    where
        T: Clone,
    {
        let mut values = Vec::default();
        for val in self.entries.values() {
            values.push(val.value.clone())
        }
        values
    }

    /// Garbage collect all timer-entries marked as deleted by application.
    pub fn gc(&mut self) -> impl Iterator<Item = T>
    where
        K: Ord,
    {
        let mut prev = self.head.as_mut();
        let mut gced = vec![];

        loop {
            match prev.take_next() {
                next @ Titem::Sentinel => {
                    prev.set_next(next);
                    break;
                }
                Titem::Timeout { key, te, mut next, .. } if te.is_deleted() => {
                    mem::drop(self.entries.remove(&key));
                    match Arc::try_unwrap(te) {
                        Ok(te) => gced.push(te.value),
                        Err(_) => unreachable!("fatal"),
                    }
                    let next = mem::replace(&mut next, Box::new(Titem::Sentinel));
                    prev.set_next(*next);
                }
                n @ Titem::Timeout { .. } => {
                    prev.set_next(n);
                    prev = match prev {
                        Titem::Head { next } => next.as_mut(),
                        Titem::Timeout { next, .. } => next.as_mut(),
                        _ => unreachable!(),
                    };
                }
                Titem::Head { .. } => unreachable!(),
            }
        }

        gced.into_iter()
    }

    /// Close this timer and return all pending entries. Some of them might have expired
    /// and others may havn't.
    pub fn close(mut self) -> impl Iterator<Item = T>
    where
        K: Ord,
    {
        let mut node = mem::replace(&mut self.head, Box::new(Titem::Sentinel));
        let mut values = vec![];
        loop {
            node = match *node {
                Titem::Head { next } => next,
                Titem::Timeout { key, te, next, .. } => {
                    mem::drop(self.entries.remove(&key));
                    match Arc::try_unwrap(te) {
                        Ok(te) => values.push(te.value),
                        Err(_) => unreachable!("fatal"),
                    }
                    next
                }
                Titem::Sentinel => {
                    break;
                }
            };
        }

        assert!(self.entries.len() == 0);

        values.into_iter()
    }

    pub fn pprint(&self)
    where
        T: fmt::Display,
    {
        let mut node = self.head.as_ref();
        loop {
            node = match node {
                Titem::Head { next } => next.as_ref(),
                Titem::Timeout { delta, te, next, .. } => {
                    let micros = time::Duration::from_micros(*delta);
                    println!("timevalue {:?} {}", micros, te.value);
                    next.as_ref()
                }
                Titem::Sentinel => break,
            };
        }
    }
}

impl<K, T> Titem<K, T> {
    fn differential(&mut self, ndelta: u64) {
        match self {
            Titem::Timeout { delta, .. } => *delta = *delta - ndelta,
            _ => unreachable!(),
        }
    }

    fn as_mut_next(&mut self) -> &mut Titem<K, T> {
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

    fn take_next(&mut self) -> Titem<K, T> {
        match self {
            Titem::Head { next } => *mem::replace(next, Box::new(Titem::Sentinel)),
            Titem::Timeout { next, .. } => *mem::replace(next, Box::new(Titem::Sentinel)),
            _ => unreachable!(),
        }
    }

    fn set_next(&mut self, new_next: Titem<K, T>) -> Box<Titem<K, T>> {
        match self {
            Titem::Head { next } => mem::replace(next, Box::new(new_next)),
            Titem::Timeout { next, .. } => mem::replace(next, Box::new(new_next)),
            _ => unreachable!(),
        }
    }
}

struct TimerEntry<T> {
    value: T,
    secs: u64,
    deleted: AtomicBool,
}

impl<T> fmt::Display for TimerEntry<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let secs = time::Duration::from_secs(self.secs);
        write!(f, "TimerEntry<{:?},{}>", secs, self.deleted.load(SeqCst))
    }
}

impl<T> TimerEntry<T> {
    fn delete(&self) {
        self.deleted.store(true, SeqCst);
    }

    fn is_deleted(&self) -> bool {
        self.deleted.load(SeqCst)
    }
}
