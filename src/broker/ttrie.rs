use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::{borrow::Borrow, thread};

use crate::broker::Spinlock;
use crate::{v5, v5::Subscription, ClientID, IterTopicPath};

// NOTE: MQTT-Spec-v5. If the Retain Handling option is 0, any existing retained messages
// matching the Topic Filter MUST be re-sent, but Applicaton Messages MUST NOT be
// lost due to replacing the Subscription.

/// Type implement a MVCC trie for managing topic-subscriptions.
///
/// Indexed with TopicFilter and matched using TopicName.
pub struct SubscribedTrie {
    mu: Arc<Mutex<u32>>,
    stats: Stats,
    inner: Arc<Spinlock<Arc<Inner<Subscription>>>>,
}

struct Inner<V> {
    root: Arc<Node<V>>,
}

impl Default for SubscribedTrie {
    fn default() -> SubscribedTrie {
        let inner = Inner {
            root: Arc::new(Node::<Subscription>::Root { children: Vec::default() }),
        };
        let mu = Arc::new(Mutex::new(0));
        SubscribedTrie {
            mu,
            stats: Stats::default(),
            inner: Arc::new(Spinlock::new(Arc::new(inner))),
        }
    }
}

impl SubscribedTrie {
    pub fn clone(&self) -> SubscribedTrie {
        let mu = Arc::clone(&self.mu);
        SubscribedTrie {
            mu,
            stats: self.stats.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl SubscribedTrie {
    pub fn subscribe<'b, K>(&self, key: &'b K, val: Subscription) -> Option<Subscription>
    where
        K: IterTopicPath<'b>,
    {
        use std::sync::TryLockError;

        let _guard = loop {
            match self.mu.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => thread::yield_now(),
                Err(TryLockError::Poisoned(_)) => {
                    panic!("SubscribedTrie::subscribe IPCFail write lock poisoned");
                }
            }
        };

        self.do_subscribe(key, val)
    }

    pub fn unsubscribe<'a, K>(
        &self,
        key: &'a K,
        value: &Subscription,
    ) -> Option<Subscription>
    where
        K: IterTopicPath<'a>,
    {
        use std::sync::TryLockError;

        let _guard = loop {
            match self.mu.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => thread::yield_now(),
                Err(TryLockError::Poisoned(_)) => {
                    panic!("SubscribedTrie::unsubscribe IPCFail write lock poisoned");
                }
            }
        };

        self.do_unsubscr(key, value)
    }

    pub fn match_topic_name<'b, K>(&self, key: &'b K) -> Vec<Subscription>
    where
        K: IterTopicPath<'b>,
    {
        let is_dollar = key.is_dollar_topic();
        let in_levels = key.iter_topic_path();

        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };

        self.stats.lookups.fetch_add(1, SeqCst);

        let vals = root.match_topic_name(in_levels, is_dollar);
        if !vals.is_empty() {
            self.stats.hits.fetch_add(1, SeqCst);
        }

        vals
    }

    pub fn pretty_print(&self) {
        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };
        root.pretty_print("");
    }
}

impl SubscribedTrie {
    // return previous subscription for `key` owned by `client-id`.
    fn do_subscribe<'b, K>(&self, key: &'b K, value: Subscription) -> Option<Subscription>
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };

        let (root, first, replace) = root.sub(in_levels, value);
        if first {
            self.stats.count.fetch_add(1, SeqCst);
        }
        if replace.is_some() {
            self.stats.replace.fetch_add(1, SeqCst);
        }

        let inner = Inner { root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);

        replace
    }

    fn do_unsubscr<'a, K>(&self, key: &'a K, val: &Subscription) -> Option<Subscription>
    where
        K: IterTopicPath<'a>,
    {
        let in_levels = key.iter_topic_path();

        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };

        let (root, last, rmvalue) = root.unsub(in_levels, val);
        let root = root.unwrap();

        if last {
            self.stats.count.fetch_add(1, SeqCst);
        }
        if rmvalue.is_none() {
            self.stats.missing.fetch_add(1, SeqCst);
        }

        let inner = Inner { root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);

        rmvalue
    }
}

/// Type implement a MVCC trie for managing retain messages.
///
/// Indexed with TopicName and matched using TopicFilter.
pub struct RetainedTrie {
    mu: Arc<Mutex<u32>>,
    stats: Stats,
    inner: Arc<Spinlock<Arc<Inner<v5::Publish>>>>,
}

impl Default for RetainedTrie {
    fn default() -> RetainedTrie {
        let inner = Inner {
            root: Arc::new(Node::<v5::Publish>::Root { children: Vec::default() }),
        };
        let mu = Arc::new(Mutex::new(0));
        RetainedTrie {
            mu,
            stats: Stats::default(),
            inner: Arc::new(Spinlock::new(Arc::new(inner))),
        }
    }
}

impl RetainedTrie {
    pub fn clone(&self) -> RetainedTrie {
        let mu = Arc::clone(&self.mu);
        RetainedTrie {
            mu,
            stats: self.stats.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl RetainedTrie {
    pub fn set<'b, K>(&self, key: &'b K, value: v5::Publish)
    where
        K: IterTopicPath<'b>,
    {
        use std::sync::TryLockError;

        let _guard = loop {
            match self.mu.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => thread::yield_now(),
                Err(TryLockError::Poisoned(_)) => {
                    panic!("RetainedTrie::set IPCFail write lock poisoned");
                }
            }
        };

        self.do_set(key, value)
    }

    pub fn remove<'a, K>(&self, key: &'a K)
    where
        K: IterTopicPath<'a>,
    {
        use std::sync::TryLockError;

        let _guard = loop {
            match self.mu.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => thread::yield_now(),
                Err(TryLockError::Poisoned(_)) => {
                    panic!("RetainedTrie::remove IPCFail write lock poisoned");
                }
            }
        };

        self.do_remove(key)
    }

    pub fn match_topic_filter<'b, K>(&self, key: &'b K) -> Vec<v5::Publish>
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };

        self.stats.lookups.fetch_add(1, SeqCst);

        let mut vals = vec![];
        let _ = root.match_topic_filter(in_levels, &mut vals);
        if !vals.is_empty() {
            self.stats.hits.fetch_add(1, SeqCst);
        }

        vals
    }

    pub fn pretty_print(&self) {
        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };
        root.pretty_print("");
    }
}

impl RetainedTrie {
    fn do_set<'b, K>(&self, key: &'b K, value: v5::Publish)
    where
        K: IterTopicPath<'b>,
    {
        let in_levels = key.iter_topic_path();

        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };

        let (root, first) = root.set(in_levels, value);
        if first {
            self.stats.count.fetch_add(1, SeqCst);
        }

        let inner = Inner { root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);
    }

    fn do_remove<'a, K>(&self, key: &'a K)
    where
        K: IterTopicPath<'a>,
    {
        let in_levels = key.iter_topic_path();

        let root = {
            let inner = Arc::clone(&self.inner.read());
            Arc::clone(&inner.root)
        };

        let (root, missing) = root.remove(in_levels);
        let root = root.unwrap();

        if missing {
            self.stats.missing.fetch_add(1, SeqCst);
        } else {
            self.stats.count.fetch_add(1, SeqCst);
        }

        let inner = Inner { root: Arc::new(root) };
        *self.inner.write() = Arc::new(inner);
    }
}

enum Node<V> {
    Root {
        children: Vec<Arc<Node<V>>>, // sorted list of nodes
    },
    Child {
        name: String,                // can be zero-length, also the sort key for node.
        children: Vec<Arc<Node<V>>>, // sorted list of nodes
        values: Vec<V>,              // TODO: should we make this Arc<V>
    },
}

impl<V> Node<V> {
    fn new_node(name: String) -> Node<V> {
        Node::<V>::Child {
            name,
            children: Vec::default(),
            values: Vec::default(),
        }
    }

    fn cow_clone(&self) -> Node<V>
    where
        V: Clone,
    {
        match self {
            Node::Root { children } => Node::Root { children: children.clone() },
            Node::Child { name, children, values } => Node::Child {
                name: name.clone(),
                children: children.clone(),
                values: values.to_vec(),
            },
        }
    }
}

impl<V> Node<V> {
    fn as_name(&self) -> &str {
        match self {
            Node::Child { name, .. } => name.as_str(),
            _ => unreachable!(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Node::Child { children, values, .. } => {
                children.len() == 0 && values.len() == 0
            }
            Node::Root { .. } => false,
        }
    }

    fn subtree_values(&self, is_wilder: bool, acc: &mut Vec<V>)
    where
        V: Clone,
    {
        match self {
            Node::Root { children } if is_wilder => {
                // MQTT Spec. 4.7: The Server MUST NOT match Topic Filters starting
                // with a wildcard character (# or +) with Topic Names beginning with
                // a $ character. The Server SHOULD prevent Clients from using such
                // Topic Names to exchange messages with other Clients. Server
                // implementations MAY use Topic Names that start with a leading
                // $ character for other purposes.
                for child in children.iter() {
                    let b = child.as_name().as_bytes().first();
                    if !matches!(b, Some(36) /*'$'*/) {
                        child.subtree_values(false, acc);
                    }
                }
            }
            Node::Root { children } => {
                for child in children.iter() {
                    child.subtree_values(false, acc);
                }
            }
            Node::Child { children, values, .. } => {
                acc.extend(values.to_vec().into_iter());
                for child in children.iter() {
                    child.subtree_values(false, acc);
                }
            }
        };
    }

    fn pretty_print(&self, prefix: &str) {
        match self {
            Node::Root { children } => {
                println!("{}Node::Root {}", prefix, children.len());
                let prefix = format!("{}  ", prefix);
                for child in children.iter() {
                    child.pretty_print(&prefix);
                }
            }
            Node::Child { name, children, values } => {
                let (n, m) = (children.len(), values.len());
                println!("{}Node {:?} children:{} values:{}", prefix, name, n, m);
                let prefix = format!("{}  ", prefix);
                for child in children.iter() {
                    child.pretty_print(&prefix);
                }
            }
        }
    }
}

impl<V> Node<V> {
    // return (first, replace)
    // `first` is whether this is the first time a topic is subscribed.
    fn insert_value(&mut self, value: V) -> (bool, Option<V>)
    where
        V: Ord + AsRef<ClientID>,
    {
        let client_id: &ClientID = value.as_ref();
        match self {
            // TODO: this match arm is redundant, fold it with Err(off) further down.
            Node::Child { values, .. } if values.len() == 0 => {
                values.push(value);
                (true, None)
            }
            Node::Child { values, .. } => {
                // MQTT-spec-v5: If a Server receives a SUBSCRIBE packet containing a
                // Topic Filter that is identical to a Non-shared Subscription's Topic
                // Filter for the current Session, then it MUST replace that existing
                // Subscription with a new Subscription.
                match values.binary_search_by_key(&client_id, |v| v.as_ref()) {
                    Ok(off) => {
                        values.push(value);
                        let replace = Some(values.swap_remove(off));
                        (false, replace)
                    }
                    Err(off) => {
                        values.insert(off, value);
                        (false, None)
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    // return (last, removed-value)
    // `last` is the last value for this topic.
    fn remove_value(&mut self, value: &V) -> (bool, Option<V>)
    where
        V: Ord + AsRef<ClientID>,
    {
        let client_id: &ClientID = value.as_ref();
        match self {
            Node::Child { values, .. } if values.len() == 0 => (false, None),
            Node::Child { values, .. } => {
                match values.binary_search_by_key(&client_id, |v| v.as_ref()) {
                    Ok(off) => {
                        let rmvalue = values.remove(off);
                        (values.len() == 0, Some(rmvalue))
                    }
                    Err(_off) => (false, None),
                }
            }
            Node::Root { .. } => (false, None),
        }
    }

    // return (first,)
    // `first` is whether this is the first time a topic is subscribed.
    fn replace_value(&mut self, value: V) -> bool {
        match self {
            Node::Child { values, .. } if values.len() == 0 => {
                *values = vec![value];
                true
            }
            Node::Child { values, .. } => {
                *values = vec![value];
                false
            }
            _ => unreachable!(),
        }
    }

    // return (missing,)
    fn remove_topic(&mut self) -> bool {
        match self {
            Node::Child { values, .. } if values.len() == 0 => true,
            Node::Child { values, .. } if values.len() == 1 => {
                values.remove(0);
                false
            }
            Node::Child { .. } => unreachable!(),
            Node::Root { .. } => unreachable!(),
        }
    }
}

impl<V> Node<V> {
    // return (root, first, replace)
    fn sub<'a, K>(&self, mut in_levels: K, value: V) -> (Node<V>, bool, Option<V>)
    where
        K: Iterator<Item = &'a str>,
        V: Clone + Ord + AsRef<ClientID>,
    {
        let mut cow_node = self.cow_clone();

        match in_levels.next() {
            Some(in_level) => {
                let children = match &mut cow_node {
                    Node::Root { children } => children,
                    Node::Child { children, .. } => children,
                };
                let r = match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        (child.sub(in_levels, value), off)
                    }
                    Err(off) => {
                        let child = Arc::new(Node::<V>::new_node(in_level.to_string()));
                        (child.sub(in_levels, value), off)
                    }
                };
                let ((child, first, replace), off) = r;
                children.insert(off, Arc::new(child));
                (cow_node, first, replace)
            }
            None => {
                let (first, replace) = cow_node.insert_value(value);
                (cow_node, first, replace)
            }
        }
    }

    // return (root, last, removed-value)
    fn unsub<'a, K>(&self, mut key: K, value: &V) -> (Option<Node<V>>, bool, Option<V>)
    where
        K: Iterator<Item = &'a str>,
        V: Clone + Ord + AsRef<ClientID>,
    {
        let mut cow_node = self.cow_clone();

        match key.next() {
            Some(ky) => {
                let (is_root, children) = match &mut cow_node {
                    Node::Root { children } => (true, children),
                    Node::Child { children, .. } => (false, children),
                };
                match children.binary_search_by_key(&ky, |n| n.as_name()) {
                    Ok(off) => match children.remove(off).unsub(key, value) {
                        (Some(child), last, rmval) => {
                            children.insert(off, Arc::new(child));
                            (Some(cow_node), last, rmval)
                        }
                        (None, last, rmval) if is_root => (Some(cow_node), last, rmval),
                        (None, last, rmval) if cow_node.is_empty() => (None, last, rmval),
                        (None, last, rmval) => (Some(cow_node), last, rmval),
                    },
                    Err(_off) => (Some(cow_node), false, None),
                }
            }
            None => {
                let is_root = match &mut cow_node {
                    Node::Root { .. } => true,
                    Node::Child { .. } => false,
                };
                match cow_node.remove_value(value) {
                    (last, rmval) if is_root => (Some(cow_node), last, rmval),
                    (last, rmval) if cow_node.is_empty() => (None, last, rmval),
                    (last, rmval) => (Some(cow_node), last, rmval),
                }
            }
        }
    }

    // return (root, first)
    fn set<'a, K>(&self, mut in_levels: K, value: V) -> (Node<V>, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone,
    {
        let mut cow_node = self.cow_clone();

        match in_levels.next() {
            Some(in_level) => {
                let children = match &mut cow_node {
                    Node::Root { children } => children,
                    Node::Child { children, .. } => children,
                };
                let r = match children.binary_search_by_key(&in_level, |n| n.as_name()) {
                    Ok(off) => {
                        let child = children.remove(off);
                        (child.set(in_levels, value), off)
                    }
                    Err(off) => {
                        let child = Arc::new(Node::<V>::new_node(in_level.to_string()));
                        (child.set(in_levels, value), off)
                    }
                };
                let ((child, first), off) = r;
                children.insert(off, Arc::new(child));
                (cow_node, first)
            }
            None => {
                let first = cow_node.replace_value(value);
                (cow_node, first)
            }
        }
    }

    // return (root, missing)
    fn remove<'a, K>(&self, mut key: K) -> (Option<Node<V>>, bool)
    where
        K: Iterator<Item = &'a str>,
        V: Clone,
    {
        let mut cow_node = self.cow_clone();

        match key.next() {
            Some(ky) => {
                let (is_root, children) = match &mut cow_node {
                    Node::Root { children } => (true, children),
                    Node::Child { children, .. } => (false, children),
                };
                match children.binary_search_by_key(&ky, |n| n.as_name()) {
                    Ok(off) => match children.remove(off).remove(key) {
                        (Some(child), missing) => {
                            children.insert(off, Arc::new(child));
                            (Some(cow_node), missing)
                        }
                        (None, missing) if is_root => (Some(cow_node), missing),
                        (None, missing) if cow_node.is_empty() => (None, missing),
                        (None, missing) => (Some(cow_node), missing),
                    },
                    Err(_off) => (Some(cow_node), true),
                }
            }
            None => match cow_node.remove_topic() {
                missing if cow_node.is_empty() => (None, missing),
                missing => (Some(cow_node), missing),
            },
        }
    }

    fn match_topic_name<'a, I>(&self, mut in_levels: I, is_dollar: bool) -> Vec<V>
    where
        I: Iterator<Item = &'a str> + Clone,
        V: Clone,
    {
        let in_level = match in_levels.next() {
            None => match self {
                Node::Root { .. } => return Vec::default(),
                Node::Child { values, children, .. } => {
                    let mut acc: Vec<V> = values.to_vec();
                    for child in children.iter().map(|x| x.as_ref()) {
                        match child {
                            Node::Child { name, values, .. } if name == "#" => {
                                acc.extend_from_slice(values)
                            }
                            _ => (),
                        }
                    }
                    return acc;
                }
            },
            Some(in_level) => in_level,
        };

        let children: Box<dyn Iterator<Item = &Arc<Node<V>>>> = match self {
            Node::Root { children } if is_dollar => Box::new(
                // MQTT Spec. 4.7: The Server MUST NOT match Topic Filters starting
                // with a wildcard character (# or +) with Topic Names beginning with
                // a $ character. The Server SHOULD prevent Clients from using such
                // Topic Names to exchange messages with other Clients. Server
                // implementations MAY use Topic Names that start with a leading
                // $ character for other purposes.
                children.iter().filter(|child| !matches!(child.as_name(), "#" | "+")),
            ),
            Node::Root { children } => Box::new(children.iter()),
            Node::Child { children, .. } => Box::new(children.iter()),
        };

        let mut acc = vec![];
        for child in children {
            match match_level(in_level, child.as_name()) {
                Match::All => {
                    if let Node::Child { values, .. } = child.borrow() {
                        acc.extend(values.to_vec().into_iter());
                    }
                }
                Match::True => {
                    let in_levels = in_levels.clone();
                    let values = child.match_topic_name(in_levels, is_dollar);
                    acc.extend(values.into_iter())
                }
                Match::False => (),
            }
        }

        acc
    }

    // return (optional list of publishes, multi_level)
    fn match_topic_filter<'a, I>(&self, mut in_levels: I, acc: &mut Vec<V>) -> bool
    where
        I: Iterator<Item = &'a str> + Clone,
        V: Clone,
    {
        let (in_level, is_plus) = match in_levels.next() {
            None => match self {
                Node::Root { .. } => return false,
                Node::Child { values, .. } => {
                    acc.extend(values.to_vec().into_iter());
                    return false;
                }
            },
            Some("#") => match self {
                Node::Root { .. } => {
                    self.subtree_values(true, acc);
                    return true;
                }
                Node::Child { .. } => {
                    self.subtree_values(false, acc);
                    return true;
                }
            },
            Some("+") => match self {
                Node::Root { .. } => ("+", true),
                Node::Child { .. } => ("+", true),
            },
            Some(in_level) => (in_level, false),
        };

        let children: Box<dyn Iterator<Item = &Arc<Node<V>>>> = match self {
            Node::Root { children } if is_plus => {
                Box::new(children.iter().filter(|child| {
                    !matches!(child.as_name().as_bytes().first(), Some(36) /*'$'*/)
                }))
            }
            Node::Root { children } => Box::new(children.iter()),
            Node::Child { children, .. } => Box::new(children.iter()),
        };

        for child in children {
            let name = child.as_name();
            match match_level(in_level, name) {
                Match::True => {
                    // println!("Match::True {} {} {}", in_level, name, acc.len());
                    let in_levels = in_levels.clone();
                    child.match_topic_filter(in_levels, acc)
                }
                Match::False => {
                    // println!("Match::False {:?} {:?}", in_level, name);
                    false
                }
                Match::All => unreachable!(),
            };
        }

        false
    }
}

enum Match {
    All,
    True,
    False,
}

// (level_match, multi_level_match)
// input key must have be already validated !!
fn match_level(in_lvl: &str, trie_level: &str) -> Match {
    match (in_lvl, trie_level) {
        ("#", _) => Match::All,
        (_, "#") => Match::All,
        ("+", _) => Match::True,
        (_, "+") => Match::True,
        (in_lvl, trie_level) if compare_level(in_lvl, trie_level) => Match::True,
        (_, _) => Match::False,
    }
}

fn compare_level(in_level: &str, trie_level: &str) -> bool {
    if in_level == trie_level {
        true
    } else {
        false
    }
}

#[derive(Clone, Default)]
pub struct Stats {
    // number of topics in the trie.
    pub count: Arc<AtomicUsize>,
    // number repeated inserts of same topic.
    pub replace: Arc<AtomicUsize>,
    // number of missing topics removed.
    pub missing: Arc<AtomicUsize>,
    // total number of matches
    pub lookups: Arc<AtomicUsize>,
    // number of hits
    pub hits: Arc<AtomicUsize>,
}

/// A simple matcher, that confirms to Section 4.7 of the MQTT v5 spec. This match
/// algorithm is commutative between TopicName and TopicFilter.
pub fn route_match<S: AsRef<str>>(this: &str, index: &[S]) -> Vec<String> {
    let mut outs = Vec::default();
    for other in index.iter() {
        let other: &str = other.as_ref();
        match (this.chars().next(), other.chars().next()) {
            (None, _) => return Vec::default(),
            (_, None) => return Vec::default(),
            (Some('$'), Some('#')) => continue,
            (Some('$'), Some('+')) => continue,
            (Some('#'), Some('$')) => continue,
            (Some('+'), Some('$')) => continue,
            (_, _) => (),
        }
        let mut iter1 = this.split('/');
        let mut iter2 = other.split('/');
        let _b = loop {
            match (iter1.next(), iter2.next()) {
                (Some(l1), Some(l2)) => match match_level(l1, l2) {
                    Match::All => {
                        // println!("other1 {}", other);
                        outs.push(other.to_string());
                        break true;
                    }
                    Match::False => break false,
                    Match::True => (),
                },
                (None, Some("#")) => {
                    // println!("other2 {}", other);
                    outs.push(other.to_string());
                    break true;
                }
                (None, Some(_)) => break false,
                (Some("#"), None) => {
                    // println!("other3 {}", other);
                    outs.push(other.to_string());
                    break true;
                }
                (Some(_), None) => break false,
                (None, None) => {
                    // println!("other4 {}", other);
                    outs.push(other.to_string());
                    break true;
                }
            }
        };
    }

    // println!("outs {:?}", outs);
    outs
}
